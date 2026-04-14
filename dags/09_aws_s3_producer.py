# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# 2. 환경변수 설정
# 827913617635 : 루트 계정 ID
# 리전 : ap-northeast-2
# 2-1. 버킷명
BUCKET_NAME = "de-ai-12-827913617635-ap-northeast-2-an"

# 2-2. 업로드 경로(prefix)
S3_PREFIX = "sensor-data"

# 2-3. 업로드할 파일명 / 로컬 경로
# 파일명은 매 수행시 고유하게 생성하여 중복 방지
FILE_NAME = "sensor_data_{{ ts_nodash }}.json"
LOCAL_PATH = f"/opt/airflow/dags/data/{FILE_NAME}"
S3_KEY = f"{S3_PREFIX}/{FILE_NAME}"


def _check_s3(**kwargs):
    # S3Hook을 통해서 실제 파일이 존재하는지 체크
    hook = S3Hook(aws_conn_id='aws_default')

    target_key = f"{S3_PREFIX}/sensor_data_{kwargs['ts_nodash']}.json"
    is_exist = hook.check_for_key(
        key=target_key,
        bucket_name=BUCKET_NAME
    )

    if not is_exist:
        raise ValueError('업로드 실패')

    logging.info(f'업로드 확인 완료 -> bucket:{BUCKET_NAME}, key:{target_key}')


# 3. DAG 정의
with DAG(
    dag_id="09_aws_s3_producer",
    description="센서 데이터를 파일로 생성하여 S3에 업로드하는 producer DAG",
    default_args={
        'owner': 'de_2team_manager',
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    },
    schedule_interval='@daily',
    start_date=datetime(2026, 2, 25),
    catchup=False,
    tags=['aws', 's3', 'producer', 'sensor'],
) as dag:
    # 4. TASK 정의
    # 센서 더미 데이터를 json 파일로 생성
    task_create_file = BashOperator(
        task_id="create_file",
        bash_command="""
        mkdir -p /opt/airflow/dags/data && \
        python - <<'PY'
import json
import random
from datetime import datetime

file_path = "/opt/airflow/dags/data/sensor_data_{{ ts_nodash }}.json"

data = [
    {
        "sensor_id": f"SENSOR_{i+1}",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "temperature": round(random.uniform(20.0, 150.0), 2),
        "status": "on"
    }
    for i in range(10)
]

with open(file_path, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(file_path)
PY
        """
    )

    task_upload_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        filename=LOCAL_PATH,
        dest_key=S3_KEY,
        dest_bucket=BUCKET_NAME,
        aws_conn_id='aws_default',
        replace=False
    )

    task_check_s3 = PythonOperator(
        task_id="check_s3",
        python_callable=_check_s3
    )

    # 5. 의존성
    task_create_file >> task_upload_to_s3 >> task_check_s3