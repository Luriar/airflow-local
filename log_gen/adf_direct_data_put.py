'''
- Amazon Data Firehous(ADF)에게 direct로 데이터를 put 샘플
'''
import boto3
import json
from datetime import datetime

# 2. 환경변수
ACCESS_KEY = ''
SECRET_KEY = ''
REGION = 'ap-northeast-2'

# 3. Firehose 클라이언트 생성
def get_client( service_name = 'firehose', is_in_aws = True):
    if not is_in_aws:
        # AWS 외부에서 진행
        session = boto3.Session(aws_access_key_id=ACCESS_KEY,
                                aws_secret_access_key=SECRET_KEY,
                                region_name=REGION
                                )
        return session.client(service_name)

    # AWS 내부에서 진행
    return boto3.client(service_name, region_name = REGION)

firehose = get_client()
print(firehose)

# 4. 로그 생성 및 ADF 발송
from run import make_one_log

# 5. 로그 1개 생성 -> adf 발송 함수
def send_log():
    # 5-1. 로그 1개 생성
    response = firehose.put_record(
        # 어디로? -> firehose 스트림 (본인 것)
        DeliveryStreamName = 'de-ai-12-an2-kdf-log-to-s3 ',
        # 데이터
        Record = {'Data': make_one_log() + "\n"} # 로그 데이터를 한 줄씩 적재
    )
    print(f'전송결과 : {response}') # 응답코드 -> 200 ok
    pass

# 6. 10번 로그 생성 발송
for i in range(10):
    send_log()
