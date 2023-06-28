import unittest
from unittest.mock import patch, MagicMock
import json
from collections import defaultdict

from datetime import datetime
from datetime import timedelta

import requests
import logging




class TestETL(unittest.TestCase):
    
    def test_etl(self, mock_get_Redshift_connection, mock_get):
        
        # 테스트용 데이터
        mock_response = MagicMock()
        mock_response.content = json.dumps({
            "response": {
                "header": {
                    "resultCode": "00"
                },
                "body": {
                    "items": {
                        "item": [{'baseDate': '20230628', 'baseTime': '0630', 'category': 'LGT', 'fcstDate': '20230628', 'fcstTime': '0700', 'fcstValue': '0', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'LGT', 'fcstDate': '20230628', 'fcstTime': '0800', 'fcstValue': '0', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'LGT', 'fcstDate': '20230628', 'fcstTime': '0900', 'fcstValue': '0', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'LGT', 'fcstDate': '20230628', 'fcstTime': '1000', 'fcstValue': '0', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'LGT', 'fcstDate': '20230628', 'fcstTime': '1100', 'fcstValue': '0', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'LGT', 'fcstDate': '20230628', 'fcstTime': '1200', 'fcstValue': '0', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'PTY', 'fcstDate': '20230628', 'fcstTime': '0700', 'fcstValue': '0', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'PTY', 'fcstDate': '20230628', 'fcstTime': '0800', 'fcstValue': '0', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'PTY', 'fcstDate': '20230628', 'fcstTime': '0900', 
'fcstValue': '0', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'PTY', 'fcstDate': '20230628', 'fcstTime': '1000', 'fcstValue': '0', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'PTY', 'fcstDate': '20230628', 'fcstTime': '1100', 'fcstValue': '0', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'PTY', 'fcstDate': '20230628', 'fcstTime': '1200', 'fcstValue': '0', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'RN1', 'fcstDate': '20230628', 'fcstTime': '0700', 'fcstValue': '강수없음', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'RN1', 'fcstDate': '20230628', 'fcstTime': '0800', 'fcstValue': '강수없음', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'RN1', 'fcstDate': '20230628', 'fcstTime': '0900', 'fcstValue': '강수없음', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'RN1', 'fcstDate': '20230628', 'fcstTime': '1000', 'fcstValue': '강수없음', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'RN1', 'fcstDate': '20230628', 'fcstTime': '1100', 'fcstValue': '강수없음', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'RN1', 'fcstDate': '20230628', 'fcstTime': '1200', 'fcstValue': '강수없음', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'SKY', 'fcstDate': '20230628', 'fcstTime': '0700', 'fcstValue': '4', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'SKY', 'fcstDate': '20230628', 'fcstTime': '0800', 'fcstValue': '3', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'SKY', 'fcstDate': '20230628', 'fcstTime': '0900', 'fcstValue': '3', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'SKY', 'fcstDate': '20230628', 'fcstTime': '1000', 'fcstValue': '3', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'SKY', 'fcstDate': '20230628', 'fcstTime': '1100', 'fcstValue': '1', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'SKY', 'fcstDate': '20230628', 'fcstTime': '1200', 'fcstValue': '1', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'T1H', 'fcstDate': '20230628', 'fcstTime': '0700', 'fcstValue': '23', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'T1H', 'fcstDate': '20230628', 'fcstTime': '0800', 'fcstValue': '23', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'T1H', 'fcstDate': '20230628', 'fcstTime': '0900', 'fcstValue': '24', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'T1H', 'fcstDate': '20230628', 'fcstTime': '1000', 'fcstValue': '24', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 
'category': 'T1H', 'fcstDate': '20230628', 'fcstTime': '1100', 'fcstValue': '25', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'T1H', 'fcstDate': '20230628', 'fcstTime': '1200', 'fcstValue': '26', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'REH', 'fcstDate': '20230628', 'fcstTime': '0700', 'fcstValue': '85', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'REH', 'fcstDate': '20230628', 'fcstTime': '0800', 'fcstValue': '85', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'REH', 'fcstDate': '20230628', 'fcstTime': '0900', 'fcstValue': '85', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'REH', 'fcstDate': '20230628', 'fcstTime': '1000', 'fcstValue': '90', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'REH', 'fcstDate': '20230628', 'fcstTime': '1100', 'fcstValue': '85', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'REH', 'fcstDate': '20230628', 'fcstTime': '1200', 'fcstValue': '80', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'UUU', 'fcstDate': '20230628', 'fcstTime': '0700', 'fcstValue': '0.3', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'UUU', 'fcstDate': '20230628', 'fcstTime': '0800', 'fcstValue': '0.5', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'UUU', 'fcstDate': '20230628', 'fcstTime': '0900', 'fcstValue': '1', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'UUU', 'fcstDate': '20230628', 'fcstTime': '1000', 'fcstValue': '1.1', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'UUU', 'fcstDate': '20230628', 'fcstTime': '1100', 
'fcstValue': '0.9', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'UUU', 'fcstDate': '20230628', 'fcstTime': '1200', 'fcstValue': '1.3', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'VVV', 'fcstDate': '20230628', 'fcstTime': '0700', 'fcstValue': '0.1', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'VVV', 'fcstDate': '20230628', 'fcstTime': '0800', 'fcstValue': '0.4', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'VVV', 'fcstDate': '20230628', 'fcstTime': '0900', 'fcstValue': '1.1', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'VVV', 'fcstDate': '20230628', 'fcstTime': '1000', 'fcstValue': '1.5', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'VVV', 'fcstDate': '20230628', 'fcstTime': '1100', 'fcstValue': '1.6', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'VVV', 'fcstDate': '20230628', 'fcstTime': '1200', 'fcstValue': '1.8', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'VEC', 'fcstDate': '20230628', 'fcstTime': '0700', 'fcstValue': '254', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'VEC', 'fcstDate': '20230628', 'fcstTime': '0800', 'fcstValue': '229', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 
'baseTime': '0630', 'category': 'VEC', 'fcstDate': '20230628', 'fcstTime': '0900', 'fcstValue': '222', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'VEC', 'fcstDate': '20230628', 'fcstTime': '1000', 'fcstValue': '216', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'VEC', 'fcstDate': '20230628', 'fcstTime': '1100', 'fcstValue': '208', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'VEC', 'fcstDate': '20230628', 'fcstTime': '1200', 'fcstValue': '216', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'WSD', 'fcstDate': '20230628', 'fcstTime': '0700', 'fcstValue': '0', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'WSD', 'fcstDate': '20230628', 'fcstTime': '0800', 'fcstValue': '1', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'WSD', 'fcstDate': '20230628', 'fcstTime': '0900', 'fcstValue': '1', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'WSD', 'fcstDate': '20230628', 'fcstTime': '1000', 'fcstValue': '2', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'WSD', 'fcstDate': '20230628', 'fcstTime': '1100', 'fcstValue': '2', 'nx': 55, 'ny': 127}, {'baseDate': '20230628', 'baseTime': '0630', 'category': 'WSD', 'fcstDate': '20230628', 'fcstTime': '1200', 'fcstValue': '2', 'nx': 55, 'ny': 127}]  # 실제 응답과 유사한 데이터
                    }
                }
            }
        })
        mock_get.return_value = mock_response
        
        mock_cursor = MagicMock()
        mock_get_Redshift_connection.return_value = mock_cursor

        # etl 함수를 호출
        etl('schema', 'table', '2023-06-28T00:00:00Z', 'api_key', 'weather_url')

        # HTTP 요청이 올바르게 이루어졌는지 확인
        mock_get.assert_called_once_with('weather_url', params={'serviceKey' : 'api_key', 'pageNo' : '1', 'numOfRows' : '1000', 'dataType' : 'JSON', 'base_date' : '20230628', 'base_time' : '0000', 'nx' : '55', 'ny' : '127'}, timeout=30)

        # 데이터베이스 쿼리가 올바르게 실행되었는지 확인
        # (예를 들어, 원하는 쿼리를 실행했는지 확인하는 등의 추가적인 단언문을 추가할 수 있습니다.)
        assert mock_cursor.execute.call_count >= 1




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
            if d['fcstValue'] == '강수없음':
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
    RN1 int,
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



if __name__ == '__main__':
    unittest.main()
