'''
- ma 에서 silver 단계 처리
- 스케쥴 ( 10 * * * * )
    - firehose에서 버퍼 시간을 최대 3분(180초)로 구성 -> 3분 이후부터는 스케쥴 가동 가능함
    - 보수적으로 10분에 작업하도록 구성
- 처리할 데이터 (flatter, 파생변수, 컬럼명변경) 전처리 수행(sql을 통해)
    - event_id
    - event_time -> event_timestamp
    - data.user_id
    - data.item_id
    - data.price
    - data.qty
    - (data.price * data.qty) as total_price
    - data.store_id
    - source_id
    - user_agent
    - dt (year-month-day)
    - hour as hr
- 작업 (silver 테이블 삭제 -> ctas)
'''

# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaHook

# 2. 환경변수
DATABASE_BRONZE = 'de-ai-12-ma-bronze-db'
DATABASE_SILVER = 'de-ai-12-ma-silver-db'
SILVER_S3_PATH = 's3://de-ai-12-827913617635-ap-northeast-2-an/medallion/silver/'
ATHENA_RESULTS = 's3://de-ai-12-827913617635-ap-northeast-2-an/atehna-results/'


# 3. DAG 정의

    # 4. task 정의 (2개)

    # 5. 의존성(injection) 구성