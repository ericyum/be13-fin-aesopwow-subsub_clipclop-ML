print("여긴 지옥이야...")

"""
    controller : 요청을 받고 응답을 반환하는 HTTP 엔드포인트를 정의
    model : 머신러닝 모델을 정의하고 관리(기존에 생각했던 model하고 다릅니다.)
    Service : controller에서 받은 데이터를 처리하고 필요한 비즈니스 로직
    dto : 요청(Request)과 응답(Response)의 데이터 형태를 정의
    repository : 데이터를 조회하거나 저장하는 로직을 담당하고 S3와 같은 외부 서비스나 파일 시스템에 데이터를 저장하거나 가져옴
    __init__.py :
        1) Flask 애플리케이션에서 Blueprint를 등록하는 등의 설정을 할 수 있습니다.
        2) Basic 패키지 내의 다양한 모듈을 하나로 묶어 Flask 애플리케이션에서 사용될 수 있도록 초기화합니다.
"""
