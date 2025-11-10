# main_worker.py

import redis
import psycopg2
import sys

# 설정 및 DB 함수만 import
from config import REDIS_SETTINGS
from db_utils import (
    get_db_connection,
    find_new_job,
    find_normalization_key,
    update_normalization_hit_count,
    insert_new_normalization_entry,
    update_log_status
)
# 정규화 모듈 import
from text_normalizer import TextNormalizer


# --- '작업 처리' 함수 ---
def process_new_queries(db_conn, normalizer: TextNormalizer):
    with db_conn.cursor() as cursor:
        while True:
            try:
                job = find_new_job(cursor)
                if not job:
                    print(f"--- 처리할 새 질의가 없거나, 다른 프로세스가 처리 중입니다. ---")
                    break

                print(f"--- 1개의 새 질의를 DB에서 추출하여 처리 시작 ---")
                log_id, raw_query = job

                print(f"\n[정제 시작] log_id: {log_id}, raw_query: '{raw_query}'")
                normalized_query = normalizer.normalize(raw_query, verbose=True)
                print(f"[정제 완료] final_key: '{normalized_query}'")

                final_key = normalized_query
                is_hit = True
                
                normalization_id = None
                existing_key = find_normalization_key(cursor, final_key)
                if existing_key:
                    normalization_id = existing_key[0]
                    update_normalization_hit_count(cursor, normalization_id)
                    print(f"  -> [DB] 기존 정제 키 ID {normalization_id}의 hit_count 증가")
                else:
                    normalization_id = insert_new_normalization_entry(cursor, final_key)
                    print(f"  -> [DB] 신규 정제 키 ID {normalization_id} 생성 ('{final_key}')")

                update_log_status(cursor, log_id, is_hit, normalization_id)
                print(f"  -> [DB] query_logs ID {log_id} 상태 업데이트 완료 (is_hit={is_hit})")

                db_conn.commit()
                print(f"\n--- 작업 완료 (log_id: {log_id}) ---")

            except Exception as e:
                print(f"❌ 작업 처리 중 심각한 오류 발생 (log_id: {locals().get('log_id','?')}): {e}", file=sys.stderr)
                if db_conn:
                    db_conn.rollback()
                raise



# --- '메인' 실행부 ---
if __name__ == "__main__":
    
    db_conn = None
    
    try:
        # --- A. PostgreSQL 연결 ---
        print("\n--- PostgreSQL '작업용' 연결 시도... ---")
        db_conn = get_db_connection() 
        if not db_conn:
            sys.exit(1) # DB 연결 실패 시 종료
        print("✅ PostgreSQL '작업용' 연결 성공")

        # --- B. [신규 V3] 정규화 모듈 준비 ---
        print("\n--- 텍스트 정규화 모듈(TextNormalizer) 초기화 중... ---")
        # normalizer가 시작 시 Redis에 연결하고, Kiwipiepy를 로드하고, 규칙을 컴파일합니다.
        try:
            normalizer = TextNormalizer()
            print(f"✅ 정규화 모듈 준비 완료\n")
        except redis.RedisError as e:
            print(f"❌ 정규화 모듈 초기화 실패 (Redis 연결 오류): {e}", file=sys.stderr)
            sys.exit(1)
        except ImportError as e:
            print(f"❌ 정규화 모듈 초기화 실패: 'kiwipiepy' 라이브러리가 설치되었는지 확인하세요.", file=sys.stderr)
            print(f"   (오류: {e})", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"❌ 정규화 모듈 초기화 실패: {e}", file=sys.stderr)
            sys.exit(1)

        # --- 본 '작업' 실행 ---
        # db_conn과 초기화된 normalizer 객체를 전달
        process_new_queries(db_conn, normalizer)

    except KeyboardInterrupt:
        print("\n\n--- 작업이 사용자에 의해 중지되었습니다. ---")
    except Exception as e:
        print(f"\n--- 메인 스레드에서 처리되지 않은 예외 발생: {e} ---", file=sys.stderr)
    finally:
        # --- D. 연결 종료 ---
        if db_conn:
            db_conn.close()
            print("\n--- PostgreSQL 연결 종료 ---")
