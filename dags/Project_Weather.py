from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

from datetime import datetime
from datetime import timedelta

import requests
import logging
import json


def get_Redshift_connection():
    # autocommit is False by default
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()



def etl(schema, table, execution_date, api_key, weather_url):
    # `execution_date`를 datetime 객체로 변환합니다.
    logging.info(f"execution_date: {execution_date}")
    execution_date = datetime.fromisoformat(execution_date.replace("Z", "+00:00"))

    # 년월일 스트링으로 변환합니다.
    base_date = execution_date.strftime('%Y%m%d')

    # 시분 스트링으로 변환합니다.
    base_time = execution_date.strftime('%H%M')

    logging.info(f"base_date: {base_date}")
    logging.info(f"base_time: {base_time}")

    url = weather_url
    params ={'serviceKey' : api_key, 'pageNo' : '1', 'numOfRows' : '1000', 'dataType' : 'JSON', 'base_date' : base_date, 'base_time' : base_time, 'nx' : '55', 'ny' : '127' }


    response = requests.get(url, params=params, timeout=30)
    data = json.loads(response.content)
    ret =[]

    if data['response']['header']['resultCode'] == '00':
        data = data['response'].get('body',"")['items']['item']

        for d in data:
            day = datetime.strptime(d["baseDate"]+d["fcstTime"],'%Y%m%d%H%M')

            if d["category"] == 'T1H':
                ret.append("('{}', {})".format(day, d["fcstValue"]))

            print(ret)

    logging.info(f"ret: {ret}")


    cur = get_Redshift_connection()
    
    # 원본 테이블이 없다면 생성
    create_table_sql = f"""CREATE TABLE IF NOT EXISTS {schema}.{table} (
    date timestamp,
    temp int,
    created_date timestamp default GETDATE()
);"""
    logging.info(create_table_sql)

    # 임시 테이블 생성
    create_t_sql = f"""CREATE TEMP TABLE t AS SELECT * FROM {schema}.{table};"""
    logging.info(create_t_sql)
    try:
        cur.execute(create_table_sql)
        cur.execute(create_t_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    # 임시 테이블 데이터 입력
    if ret:
        insert_sql = f"INSERT INTO t VALUES " + ",".join(ret)
        logging.info(insert_sql)
        try:
            cur.execute(insert_sql)
            cur.execute("COMMIT;")
        except Exception as e:
            cur.execute("ROLLBACK;")
            raise

    # 기존 테이블 대체
    alter_sql = f"""DELETE FROM {schema}.{table};
        INSERT INTO {schema}.{table}
        SELECT date, temp FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY created_date DESC) seq
        FROM t
        )
        WHERE seq = 1;"""
    logging.info(alter_sql)
    try:
        cur.execute(alter_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise




dag = DAG(
    dag_id = 'Project_Weather',
    start_date = datetime(2023,6,27), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 * * * *',  # 적당히 조절
    max_active_runs = 1,
    catchup = True,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

    
etl_task = PythonOperator(
    task_id='etl_task',
    python_callable=etl,
    op_kwargs={'schema': 'kjw9684k', 'table': 'project_weather', 'execution_date': '{{ execution_date }}',
            'api_key': Variable.get("project_weather_api_key"),'weather_url': Variable.get("project_weather_url"),},
    dag=dag
)

etl_task