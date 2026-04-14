# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import json
import pandas as pd

# 2. 환경변수 설정
BUCKET_NAME = "de-ai-12-827913617635-ap-northeast-2-an"
S3_PREFIX = "sensor-data"
S3_KEY_PATTERN = f"{S3_PREFIX}/sensor_data_*.json"


def _process_s3_files(**kwargs):
    # S3 파일 조회 -> 처리 -> MySQL 적재
    # 삭제는 S3DeleteObjectsOperator가 담당
    s3_hook = S3Hook(aws_conn_id='aws_default')
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')

    keys = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=S3_PREFIX) or []

    target_keys = sorted(
        [
            key for key in keys
            if key.endswith('.json') and key.split('/')[-1].startswith('sensor_data_')
        ]
    )

    if not target_keys:
        raise ValueError('처리할 파일 없음')

    conn = mysql_hook.get_conn()
    processed_keys = []
    total_inserted = 0

    try:
        with conn.cursor() as cursor:
            sql = '''
                insert into sensor_readings
                ( sensor_id, timestamp, temperature_c, temperature_f )
                values ( %s, %s, %s, %s )
            '''

            for key in target_keys:
                raw_text = s3_hook.read_key(key=key, bucket_name=BUCKET_NAME)
                data = json.loads(raw_text)
                df = pd.DataFrame(data)

                logging.info(f'S3 파일 처리 시작 -> key:{key}, row_count:{len(df)}')

                # 100도 미만만 적재
                target_df = df[df['temperature'] < 100].copy()

                if not target_df.empty:
                    target_df['temperature_f'] = (target_df['temperature'] * 9 / 5) + 32

                    params = [
                        (
                            row['sensor_id'],
                            row['timestamp'],
                            row['temperature'],
                            row['temperature_f']
                        )
                        for _, row in target_df.iterrows()
                    ]

                    logging.info(f'입력할 데이터(파라미터) {params}')
                    cursor.executemany(sql, params)
                    total_inserted += len(params)
                else:
                    logging.info(f'적재 대상 없음 -> key:{key}')

                # 처리 성공한 파일만 삭제 대상으로 등록
                processed_keys.append(key)

            conn.commit()
            logging.info(f'MySQL 적재 완료, 총 적재 건수:{total_inserted}')
            logging.info(f'삭제 대상 key 목록:{processed_keys}')

    except Exception as e:
        logging.exception(f'S3 소비 처리 오류 : {e}')
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()
            logging.info('mysql 연결 종료 (뒷정리)')

    # 다음 task(S3DeleteObjectsOperator)로 전달
    return processed_keys


def _check_cleanup(**kwargs):
    hook = S3Hook(aws_conn_id='aws_default')
    keys = hook.list_keys(bucket_name=BUCKET_NAME, prefix=S3_PREFIX) or []

    remain_keys = [
        key for key in keys
        if key.endswith('.json') and key.split('/')[-1].startswith('sensor_data_')
    ]

    if remain_keys:
        raise ValueError(f'정리 실패, 남은 파일 존재: {remain_keys}')

    logging.info('S3 정리 완료 -> 대상 파일 없음')


# 3. DAG 정의
with DAG(
    dag_id="09_aws_s3_consumer",
    description="S3 업로드 파일을 감지하여 처리하고 MySQL 적재 후 삭제하는 consumer DAG",
    default_args={
        'owner': 'de_2team_manager',
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    },
    schedule_interval='@daily',
    start_date=datetime(2026, 2, 25),
    catchup=False,
    tags=['aws', 's3', 'consumer', 'sensor', 'mysql'],
) as dag:

    task_create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="mysql_default",
        sql='''
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id INT AUTO_INCREMENT PRIMARY KEY,
                sensor_id VARCHAR(50),
                timestamp DATETIME,
                temperature_c FLOAT,
                temperature_f FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        '''
    )

    task_wait_for_file = S3KeySensor(
        task_id="wait_for_file",
        bucket_name=BUCKET_NAME,
        bucket_key=S3_KEY_PATTERN,
        wildcard_match=True,
        aws_conn_id='aws_default',
        poke_interval=30,
        timeout=60 * 10
    )

    task_process_s3_files = PythonOperator(
        task_id="process_s3_files",
        python_callable=_process_s3_files
    )

    task_delete_s3_files = S3DeleteObjectsOperator(
        task_id="delete_s3_files",
        bucket=BUCKET_NAME,
        keys="{{ ti.xcom_pull(task_ids='process_s3_files') }}",
        aws_conn_id='aws_default'
    )

    task_check_cleanup = PythonOperator(
        task_id="check_cleanup",
        python_callable=_check_cleanup
    )

    task_create_table >> task_wait_for_file >> task_process_s3_files >> task_delete_s3_files >> task_check_cleanup