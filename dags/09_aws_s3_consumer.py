'''
- 평시 -> 잠복하듯이 센서를 켜고 대기중
- 특정 버킷 혹은 버킷내 공간을 감시(sensor) -> 파일(객체등) 업로드 -> 감지 -> DAG 작동
- 렌터가 반납 -> 개인 촬영 -> 업로드 -> s3 -> 트리거 작동 -> 데이터 추출 전처리 -> AI 모델 전달 -> 추론 -> 평가 -> 금액 x원 산정
    - 사용자가 언제 이런 행위를 할지 아무도 모름 -> 의외성 -> 소카 모델 확인
'''

# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

# 2. 환경변수 -> 특정 버킷내에 특정 키가 변화가 왔는지만 궁금함 -> 필요한 정보만 세팅
# 버킷명 : producer와 동일해야 함
BUCKET_NAME = "de-ai-30-827913617635-ap-northeast-2-an"

# producer가 올린 키와 동일해야 함
FILE_NAME = 'sensor_data.csv'
S3_KEY = f'income/{FILE_NAME}'


def _reading_data(**kwargs):
    # 1. S3Hook 생성
    hook = S3Hook(aws_conn_id='aws_default') # 연결

    # 2. S3에서 파일 읽기
    data = hook.read_key(
        key=S3_KEY,
        bucket_name=BUCKET_NAME
    )

    # 로그 출력
    logging.info('-- 로그 출력 시작 --')
    logging.info(data)
    logging.info('-- 로그 출력 종료 --')
    pass

    # 4. mysql 연결
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    conn = mysql_hook.get_conn()

    try:
        with conn.cursor() as cursor:
            sql = '''
                insert into s3_sensor_readings
                ( id, timestamp, value )
                values ( %s, %s, %s )
            '''

            params = [
                (row['id'], row['timestamp'], row['value'])
                for _, row in df.iterrows()
            ]

            logging.info(f'입력할 데이터(파라미터) {params}')

            cursor.executemany(sql, params)
            conn.commit()
            logging.info('MySQL 적재 완료')

    except Exception as e:
        logging.exception(f'적재 오류 : {e}')
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()
            logging.info('mysql 연결 종료 (뒷정리)')


def _check_cleanup(**kwargs):
    # 삭제 후 실제로 S3에서 파일이 없어졌는지 확인
    hook = S3Hook(aws_conn_id='aws_default')

    is_exist = hook.check_for_key(
        key=S3_KEY,
        bucket_name=BUCKET_NAME
    )

    if is_exist:
        raise ValueError(f'삭제 실패 -> 아직 파일 존재: {S3_KEY}')

    logging.info(f'S3 파일 삭제 확인 완료 -> {S3_KEY}')


# 3. DAG 정의
with DAG(
    dag_id="09_aws_s3_consumer",
    description="S3 특정 버킷(사용자별로 섹션 할당 - 이름 구분 등)에 대해 데이터 변화를 감지 -> 읽기 -> 비즈니스 처리 -> 삭제(특정 위치에 보관(raw 데이터 구축))",
    default_args={
        'owner': 'de_2team_manager',
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    },
    schedule_interval=None,   # DAG는 활성화 정도만 구동, 센서 작동에 스케쥴이 필요한지 테스트
    start_date=datetime(2026, 2, 25),
    catchup=False,
    tags=['aws', 's3', 'consumer'],
) as dag:
    # 4. TASK 정의
    # 4-1 감시자(센서, 옵저버)
    task_waiting_trigger = S3KeySensor(
        task_id="task_waiting_trigger",
        # 감시 대상 설정
        bucket_key=S3_KEY,
        bucket_name=BUCKET_NAME,
        aws_conn_id='aws_default',
        # 감시 방법
        mode = 'reschedule', # 대기중에 차원 반납
        poke_interval = 10,  # 10초 간격으로 체크(주기에 따라 자원 사용 차이 발생)
        timeout = 60*10,     # 서비스 가동 후 10분 넘게 감지가 안되면 종료
    )
    # 4-2. 뭔가 작업(비즈니스)
    task_reading_data     = PythonOperator(
        task_id = "reading_data",
        python_callable = _reading_data
    )

    # 4-3. 파일 삭제(키 삭제) / 필요시 특정위치 보관 -> 뒷처리
    task_delete_data_or_backup = S3DeleteObjectsOperator(
        task_id="delete_data_or_backup",
        bucket=BUCKET_NAME,
        keys= [S3_KEY],
        aws_conn_id='aws_default',
)

    # 5. 의존성, injection
    # 센서 감지 -> 뭔가 작업 -> 키를 제거 -> 특정 위치는 최초 상태로 돌아간다
    task_waiting_trigger >> task_reading_data >> task_delete_data_or_backup