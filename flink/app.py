'''
- pip install apache-flink == 1.15.0
- 요구사항 -> raw data에서 티커별로 평균가격 10초 기준 추출 -> 다음 kinesis로 전달
- 입력 티이블, 출력 테이블, 조회 및 전송
- 표준 SQL + AWS + Flink 특징점 추가됨 형태
- Flink를 이용하면 데이터를 배치|스트리밍 등 어떤 방식이든 분석에 적합한 데이터 형태로 가공할 수 있음
- 자바|스칼라|파이썬 + SQL 결합하여 처리 가능함
'''

import os
from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    # 1. 환경 설정, 스트리밍 데이터 처리 방식에 대한 구성
    # conf = EnvironmentSettings
    #conf.a() # 인스턴스 함수
    # 데이터를 한 번에 일괄 처리 -> 배치 방식(X), 실시간(지속적) 데이터를 처리 -> 스트리밍방식(O)
    setting = EnvironmentSettings.new_instance().in_streaming_mode().build()
    # SQL과 유사한 방식으로 데이터를 다룰 수 있는 객체
    t_env = TableEnvironment(setting)

    # 2. 입력데이터에 대한 테이블 구성 (kds로부터 (INPUT) 데이터를 읽기 처리 -> 어딘가에 담는다 -> 테이블 )
    # 티커, 가격, 로그발생시간, ...

    # 3. 출력데이터에 대한 테이블 구성 (kds로부터 (OUTPUT) 데이터를 읽기 처리 -> 어딘가에 담는다 -> 테이블 )
    # 티커, 평균가격, 생성시간

    # 4. 연산(전처리, 가공, 분석(요구사항에 맞게)처리한 형태) 및 전송(kds(Output))
    pass

# 단독형 앱 -> 엔트리 포인트 표기 필요!
if __name__ == '__main__':
    main()