from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

import logging
import requests

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def get_world_info():
    # logging
    logging.info("extract started")
    # 웹에서 JSON 데이터 가져오기
    url = "https://restcountries.com/v3/all"
    response = requests.get(url)
    data = response.json()

    # 값이 없을수도 있기에 get으로 접근?
    # 처음엔 그냥 바로 접근시도 해봄.
    country = []
    population = []
    area = []
    records = []
    for line in data:
        country = line['name']['official']
        population = line['population']
        area = line['area']

        records.append([country, population, area])

    logging.info("extract finished")
    return records



@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        # 기존 테이블이 있으면 삭제
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table} (
            country VARCHAR,
            population INT,
            area FLOAT
        );")

        for r in records:
            cur.execute(f"INSERT INTO {schema}.{table} VALUES ('{r[0]}',{r[1]},{r[2]});")

        cur.execute("COMMIT;")   # cur.execute("END;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;") 
        raise
    logging.info("load done")

with DAG(
    dag_id = 'WorldInfo',
    start_date = datetime(2023,6,12),
    catchup=False,
    tags=['API'],
    schedule = '30 6 * * 6'
) as dag:
    schema = 'kjw9684k'
    table = 'worldinfo'
    records = get_world_info()
    load(schema, table, records)