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