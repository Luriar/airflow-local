'''
- 주가 로그 생성기, boto3를 이용하여 직접 연계
- 로그 -> Kinesis로 전달
- 필요 패키지
    pip install python-dotenv
'''
# 1. 모듈 가져오기
import time
import random
import json
from datetime import datetime
import boto3
from dotenv import load_dotenv
import os

# 2. 환경변수 
load_dotenv() #.env 내용을 읽어서 환경 변수로 설정
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
REGION = 'ap-northeast-2'

print( ACCESS_KEY, SECRET_KEY)
