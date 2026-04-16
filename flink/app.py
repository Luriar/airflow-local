'''
- pip install apache-flink == 1.15.0
- 요구사항 -> raw data에서 티커별로 평균가격 10초 기준 추출 -> 다음 kinesis로 전달
- 입력 티이블, 출력 테이블, 조회 및 전송
- 표준 SQL + AWS + Flink 특징점 추가됨 형태
'''

import os
from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    pass

# 단독형 앱 -> 엔트리 포인트 표기 필요!
if __name__ == '__main__':
    main()