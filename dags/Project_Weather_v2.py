from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from collections import defaultdict

from datetime import datetime
from datetime import timedelta

import requests
import logging
import json


def get_Redshift_connection():
    # autocommit is False by default
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()



def etl(schema, table, execution_date, api_key, weather_url, get_connection):
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

        # LGT, PTY, RN1(강수없음), SKY, T1H, REH, UUU, VVV, VEC, WSD
        data_dict = defaultdict(dict)
        for d in data:
            day = datetime.strptime(d["baseDate"]+d["fcstTime"],'%Y%m%d%H%M')
            if d['category'] == 'RN1' and d['fcstValue'].endswith('mm'):
                # 'mm'를 제거하고 float형으로 변환하여 저장합니다.
                data_dict[day][d['category']] = float(d['fcstValue'].replace('mm', ''))
            elif d['fcstValue'] == '강수없음':
                data_dict[day][d['category']] = '0'
            else:
                data_dict[day][d['category']] = d['fcstValue']

        # 딕셔너리를 리스트로 변환합니다.
        data_list = [(k,) + tuple(v.values()) for k, v in data_dict.items()]
        ret = [f"('{dt.strftime('%Y-%m-%d %H:%M:%S')}', {rest[0]}, {rest[1]}, {rest[2]}, {rest[3]}, {rest[4]}, {rest[5]}, {rest[6]}, {rest[7]}, {rest[8]}, {rest[9]})" for dt, *rest in data_list]
        print(ret)

    logging.info(f"ret: {ret}")


    cur = get_connection()
    
    # 원본 테이블이 없다면 생성
    # LGT, PTY, RN1, SKY, T1H, REH, UUU, VVV, VEC, WSD
    # ('2023-06-28 07:00:00', 0, 0, 0, 4, 23, 85, 0.3, 0.1, 254, 0)
    create_table_sql = f"""CREATE TABLE IF NOT EXISTS {schema}.{table} (
    date timestamp,
    LGT int,
    PTY int,
    RN1 float,
    SKY int,
    TEMP int,
    REH int,
    UUU float,
    VVV float,
    VEC int,
    WSD int,
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
    # date, LGT, PTY, RN1, SKY, TEMP, REH, UUU, VVV, VEC, WSD
    alter_sql = f"""DELETE FROM {schema}.{table};
        INSERT INTO {schema}.{table}
        SELECT date, LGT, PTY, RN1, SKY, TEMP, REH, UUU, VVV, VEC, WSD FROM (
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
    dag_id = 'Project_Weather_v2',
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
    op_kwargs={'schema': 'kjw9684k', 'table': 'project_weather_v2', 'execution_date': '{{ execution_date }}',
            'api_key': Variable.get("project_weather_api_key"),'weather_url': Variable.get("project_weather_url"),
            'get_connection': get_Redshift_connection},
    dag=dag
)

etl_task

