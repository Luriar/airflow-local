'''
- Amazon Data Firehous(ADF)에게 direct로 데이터를 put 샘플
'''
import boto3
import json
from datetime import datetime

# Firehose 클라이언트 생성
firehose = boto3.client('firehose', region_name='us-east-1')

# 전송할 데이터 준비
record = {
    'timestamp': datetime.now().isoformat(),
    'message': 'Sample data for Firehose'
}

# Firehose에 데이터 전송
response = firehose.put_record(
    DeliveryStreamName='your-delivery-stream-name',
    Record={'Data': json.dumps(record) + '\n'}
)

print(f"Record ID: {response['RecordId']}")