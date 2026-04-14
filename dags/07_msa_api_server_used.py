'''
- API 호출 과정 적용. 데이터 처리에 대한 스케쥴 구성
'''
# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import json
import requests # api 호출용, MSA 서비스 호출용

# 2. API 서버 주소
#   특정 컨테이너의 서비스명으로 URL을 조정 -> 해당 컨테이너로 요청 전달
#API_URL = 'http://127.0.0.1:8000/predict' # 현 코드가 작동중인 컨테이너 의미
API_URL = 'http://ai-api-server:8000/predict' # AI 서비스를 제공하는 컨테이너 조정

# 4-4 콜백함수 정의
def _create_dummy_data(**kwargs):
    # 차후 버전은 db 테이블에서 조회 -> 데이터 구성
    # 현재 버전은 더미 데이터를 임시 구성 xcom 전달하여 다음 task에서 사용
    users = [
        {"user_id":"C001", "income": 5000, "loan_amount": 2000},
        {"user_id":"C002", "income": 4000, "loan_amount": 5000},
        {"user_id":"C003", "income": 8000, "loan_amount": 1000}
    ]
    # xcom으로 데이터 전달
    return users

def _api_service_call(**kwargs):
    # xcom에 게시될 때는 키 값이 캌멜 표기법으로 조정, 추출할 때는 다시 스네이크 표기법 복원됨
    # 1. 이전 task의 결과물 획득 (차후 -> 데이터레이크(s3), athena, redshift, opensearch(elasticsearch의 aws버전) 등 서비스를 통해서 획득)
    ti         = kwargs['ti']
    users_data = ti.xcom_pull(task_ids='task_create_dummy_data') # xcom에서 데이터 획득
    logging.info(f'요청시 전달 데이터 {users_data}')
    # 2. 신용 평가 요청 및 응답 -> api 호출 (차후 LLM 모델과 연계 가능) -> 통신 -> I/O -> 예외처리
    try:
        # 3. post 방식으로 요청, dict 형태로 데이터 첨부 -> json 형태로 전달 (내부적으로는 객체 직렬화 처리됨)
        # 실제 서비스에서는 보안 이슈로 인증 정보, 각종 키 등을 헤더에 세팅해야 함
        res = requests.post( API_URL, json=users_data)
        # 4. 요청이 성공하면 다음으로 진행 -> 200 응답 코드 -> 스킵
        #if res.raise_for_status() == 200
        # 5. 결과 획득 (객체의 역직렬화 : json 형태 문자열 -> dict 혹은 list[ dict, ...] 형태)
        results = res.json()
        # 6. 결과 로그 출력
        logging.info(f'신용 평가 결과 획득 : {results}')
        # 7. xcom 전달
        return results
    except Exception as e:
        logging.error(f'API 호출 실패 : {e}')
        raise

    pass

def _load_users_credit(**kwargs):

    pass

# 3. DAG 정의
with DAG(
    dag_id      = "07_msa_api_server_used", 
    description = "msa 상에 특정 서비스(ai 서빙 컨셉)를 호출하여 신용평가를 수행하는 스케쥴링",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '@daily',
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['msa', 'fastapi'],
) as dag:
    # 4. Task 정의
    # 4-1. 더미 데이터 준비 -> 추후 고객 정보 저장 -> 추후 s3 업로드
    task_create_dummy_data = PythonOperator(
        task_id = "task_create_dummy_data",
        python_callable = _create_dummy_data
    )
    # 4-2. API 호출(AI 서비스 활용) -> 신용평가획득
    task_api_service_call = PythonOperator(
        task_id = "task_api_service_call",
        python_callable = _api_service_call
    )
    # 4-3. 결과저장 -> 추후 고객 정보 업데이트
    task_load_users_credit = PythonOperator(
        task_id = "task_load_users_credit",
        python_callable = _load_users_credit        
    )

    # 5. 의존성, 각 task는 xCom 통신으로 데이터 공유ㅌ
    task_create_dummy_data >> task_api_service_call >> task_load_users_credit