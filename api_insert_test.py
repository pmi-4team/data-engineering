import psycopg2
import sys
import subprocess # 다른 스크립트를 실행하기 위해 import

# --- 1. 설정 ----
DB_SETTINGS = {
    "dbname": "",
    "user": "",
    "password": "",
    "host": "",
    "port": "5432"
}

def insert_and_run_pipeline(raw_query):
    """
    1. DB에 INSERT
    2. main_worker.py 스크립트를 '호출'
    """
    sql_insert = "INSERT INTO query_logs (raw_query) VALUES (%s) RETURNING log_id;"
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_SETTINGS)
        conn.autocommit = True 
        
        with conn.cursor() as cursor:
            #  사용자로부터 들어온 질의문 INSERT
            cursor.execute(sql_insert, (raw_query,))
            log_id = cursor.fetchone()[0]
            print(f"성공: '{raw_query}'를 log_id {log_id}번으로 DB에 저장했습니다.")
            
        #    Python으로 'python main_worker.py' 명령어를 터미널에 치는 것과 동일
        print(f"--- [호출] main_worker.py 스크립트를 실행하여 방금 저장한 작업을 처리합니다... ---")
        
        # 이 코드는 'main_pipeline.py'가 끝날 때까지 기다립니다.
        subprocess.run(["python", "-u", "main_worker.py"], check=True)
        
        print(f"--- [완료] main_worker.py 작업 완료 ---")
            
    except psycopg2.Error as e:
        print(f"❌ DB 저장 실패: {e}", file=sys.stderr)
    except subprocess.CalledProcessError as e:
        print(f"❌ main_pipeline.py 실행 실패: {e}", file=sys.stderr)
    except FileNotFoundError:
        print(f"❌ 오류: 'python' 명령을 찾을 수 없거나 'main_worker.py' 파일이 없습니다.", file=sys.stderr)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # API GATEWAY로부터 들어오는 데이터, 넘기는 부분
    user_query = [
        "아이폰15 쓰는 사람들 중 서울 사는 사람이 몇명있는지 알 수 있을까>?"
    ]

    
    print("--- (API 시뮬레이터) 새 질의를 INSERT하고, 1건씩 파이프라인을 실행합니다... ---")
    for q in user_query:
        print("\n--- 새 작업 시작 ---")
        insert_and_run_pipeline(q)
