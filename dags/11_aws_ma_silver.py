'''
- ma 에서 silver 단계 처리
- 처리할 데이터 (flatter, 파생변수, 컬럼명변경) 전처리 수행(sql을 통해)
    - event_id
    - event_time -> event_timestamp
    - data.user_id
    - data.item_id
    - data.price
    - data.qty
    - (data.price * data.qty) as total_price
    - data.store_id
    - source_id
    - user_agent
    - dt (year-month-day)
    - hour as hr
'''