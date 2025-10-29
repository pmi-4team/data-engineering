import redis
import psycopg2 # PostgreSQL 드라이버 임포트
import pandas as pd
import unicodedata

# --- 사용자 설정: PostgreSQL 연결 정보 (로컬 환경에 맞게 수정됨) ---
DB_CONFIG = {
    "dbname": "?", # 수정됨
    "user": "?", # 수정됨
    "password": "?", # 수정됨
    "host": "?", # 수정됨
    "port": "5432"
}

# Redis 설정
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0
# 💡 이전 단계에서 로드한 Redis 키와 일치하도록 수정
TYPO_MAP_KEY = '?????' 

# DB 설정
TABLE_NAME = 'user_profile_answers'
# 💡 PostgreSQL 테이블의 기본 키(Primary Key) 컬럼 이름 (사용자 요청에 따라 'answer_id'로 수정됨)
PRIMARY_KEY_COLUMN = 'answer_id' 
TARGET_QUESTION_ID = '????????' # 수정할 question_id

# --- 1. Redis 맵 로드 함수 ---

def get_redis_typo_map():
    """Redis에서 표준화 맵을 불러옵니다."""
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        r.ping()
        print("✅ Redis 연결 성공 및 맵 로드 중...")
        
        typo_map = r.hgetall(TYPO_MAP_KEY)
        
        if not typo_map:
            print(f"⚠️ 경고: Redis Key '{TYPO_MAP_KEY}'에 맵 데이터가 없습니다. `redis_loader_minimal.py`를 먼저 실행해주세요.")
            return {}
        
        print(f"로드된 표준화 맵 항목 수: {len(typo_map)}개")
        return typo_map

    except redis.exceptions.ConnectionError as e:
        print(f"❌ Redis 연결 실패. 서버 상태를 확인하세요: {e}")
        return {}
    except Exception as e:
        print(f"❌ Redis 작업 중 오류 발생: {e}")
        return {}

# --- 2. DB 값 수정 ---

def update_db_values(conn, typo_map):
    """
    PostgreSQL에서 TARGET_QUESTION_ID에 해당하는 데이터를 불러와 표준화 후 업데이트합니다.
    """
    if not typo_map:
        print("❌ 표준화 맵이 없어 DB 업데이트를 건너뜁니다.")
        return

    cursor = conn.cursor()
    
    # 1. 대상 데이터 조회: question_id가 'Q5'인 행의 Primary Key와 answer_value를 가져옵니다.
    print(f"\n➡️ 테이블 '{TABLE_NAME}'에서 '{TARGET_QUESTION_ID}' 데이터 조회 중...")
    
    # PostgreSQL 구문 및 %s 플레이스홀더 사용
    select_query = f"SELECT {PRIMARY_KEY_COLUMN}, answer_value FROM {TABLE_NAME} WHERE question_id = %s"
    cursor.execute(select_query, (TARGET_QUESTION_ID,))
    rows_to_update = cursor.fetchall()
    
    update_list = []
    
    # 2. 표준화 로직 적용 및 업데이트 목록 생성
    for row_id, current_value in rows_to_update:
        # NaN 또는 None 값 건너뛰기
        if pd.isna(current_value) or current_value is None:
            continue
            
        text = str(current_value)
        
        # Redis에 저장된 키 표준화 로직과 동일하게 적용 (유니코드 정규화, 공백 제거, 소문자 변환)
        cleaned_key = unicodedata.normalize('NFC', text).strip().lower() 
        
        # Redis 맵에서 표준화된 값을 찾습니다.
        standard_value = typo_map.get(cleaned_key)
        
        # Redis 맵에 값이 있고, 그 값이 현재 DB 값과 다를 경우에만 업데이트 목록에 추가
        if standard_value is not None and standard_value != text.strip():
            # PostgreSQL executemany의 인수는 (answer_value, id) 순서여야 합니다.
            update_list.append((standard_value, row_id))
            print(f"   [변경 예정] {PRIMARY_KEY_COLUMN} {row_id}: '{current_value}' -> '{standard_value}'")


    # 3. DB에 업데이트 적용
    if update_list:
        print(f"\n🌟 총 {len(update_list)}개 행의 answer_value 업데이트 중...")
        
        # UPDATE 쿼리: PostgreSQL의 %s 플레이스홀더 사용
        update_query = f"UPDATE {TABLE_NAME} SET answer_value = %s WHERE {PRIMARY_KEY_COLUMN} = %s"
        
        # ⚠️ 이 코드는 DB 데이터를 영구적으로 수정합니다.
        cursor.executemany(update_query, update_list)
        conn.commit()
        
        print("✅ DB 업데이트 완료 및 커밋되었습니다.")
    else:
        print("✅ 업데이트할 데이터가 없습니다.")

    # 4. 업데이트된 Q5 데이터 확인 (선택 사항)
    print("\n최종 Q5 데이터 (일부 10개 확인):")
    
    # 🚨 수정된 부분: cursor.execute()와 cursor.fetchall() 분리
    final_select_query = f"SELECT {PRIMARY_KEY_COLUMN}, answer_value FROM {TABLE_NAME} WHERE question_id=%s LIMIT 10"
    cursor.execute(final_select_query, (TARGET_QUESTION_ID,))
    final_data = cursor.fetchall() 
    
    for row in final_data:
        print(f"{PRIMARY_KEY_COLUMN} {row[0]}: {row[1]}")


def main():
    # 1. Redis 맵 로드
    typo_map = get_redis_typo_map()

    # 2. DB 연결 (PostgreSQL)
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print(f"✅ PostgreSQL DB 연결 성공: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
    except Exception as e:
        print(f"❌ PostgreSQL DB 연결 오류. 설정(`DB_CONFIG`)을 확인하세요: {e}")
        return

    try:
        # 3. DB 값 표준화 및 업데이트
        update_db_values(conn, typo_map)
        
    except Exception as e:
        if conn:
             # 오류 발생 시 롤백 (안전 장치)
            conn.rollback()
        print(f"\n❌ 치명적인 오류가 발생하여 작업을 롤백했습니다.\n에러: {e}")
        
    finally:
        # 연결 종료
        if conn:
            conn.close()
            print("✅ DB 연결 종료.")


if __name__ == '__main__':
    main()
