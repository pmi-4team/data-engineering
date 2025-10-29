import redis
import json
import os
import unicodedata

# --- 설정 ---
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0
TYPO_MAP_KEY = 'typo_Q11_2_map'

# 오탈자 및 표준화 맵 데이터 (원본은 그대로)
typo_map = {
    
}
    
def normalize_and_lower_value(text):
    """유니코드 정규화 후 양쪽 공백 제거, 소문자로 변환하는 함수"""
    if text is None:
        return ""
    # 유니코드 정규화 -> 양쪽 공백 제거 -> 소문자 변환
    return unicodedata.normalize('NFC', str(text)).strip().lower()

def load_typo_map_to_redis(data_map):
    """
    정의된 맵핑 데이터를 표준화하여 Redis Hash 자료구조에 로드합니다. (키/값 모두 소문자 변환)
    """
    try:
        # Redis 클라이언트 연결
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        r.ping()
        print("✅ Redis 연결 성공.")

        pipe = r.pipeline()
        pipe.delete(TYPO_MAP_KEY)
        
        processed_count = 0
        # 맵의 모든 항목을 파이프라인에 추가
        for key, value in data_map.items():
            # 1. Key를 표준화하여 저장
            processed_key = normalize_and_lower_value(key)
            
            # --- [핵심 수정] ---
            # 2. Value도 표준화하여 소문자로 변환
            processed_value = normalize_and_lower_value(value) 
            
            pipe.hset(TYPO_MAP_KEY, processed_key, processed_value)
            
            # 디버깅을 위해 원본 값과 처리된 값을 출력 (BMW미니 -> bmw미니 확인용)
            if value != processed_value:
                 print(f"Value 표준화: '{value}' -> '{processed_value}'")

            processed_count += 1
            
        # 모든 명령어 실행
        pipe.execute()
        
        # 저장된 데이터 확인 및 검증
        total_fields = r.hlen(TYPO_MAP_KEY)
        
        print(f"\n--- Redis 로드 결과 ---")
        print(f"총 {processed_count}개의 항목을 처리하여 Redis에 저장했습니다.")
        print(f"저장된 전체 필드 수 (Key: {TYPO_MAP_KEY}): {total_fields}개")
        print(f"✅ 데이터 로드 완료. (키, 값 모두 소문자로 저장됨)")
        
        # BMW미니 -> bmw미니 로 변환되었는지 확인
        test_key = normalize_and_lower_value('미니')
        test_value_stored = r.hget(TYPO_MAP_KEY, test_key)
        print(f"테스트 확인 ('미니' 키 조회): '{test_value_stored}'")

    except redis.exceptions.ConnectionError as e:
        print(f"❌ Redis 연결 실패. 서버(Host/Port) 상태를 확인하세요: {e}")
    except Exception as e:
        print(f"❌ Redis 작업 중 오류 발생: {e}")

# --- 실행 ---
load_typo_map_to_redis(typo_map)
