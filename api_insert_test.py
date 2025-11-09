import psycopg2
import sys
import subprocess # (필수!) 다른 스크립트를 실행하기 위해 import

# --- 1. 설정  ---
DB_SETTINGS = {
    "dbname": "??",
    "user": "??",
    "password": "??",
    "host": "??",
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
    test_queries = [
       "BMW 5시리즈 보유자들은 어디 살아?",
"LG V50 휴대폰을 사용하는 사람",
"피부 만족도가 '불만족'인 사람들은 스트레스를 주로 어떻게 풀어?",
"최근 쇼핑에 돈을 많이 쓴 사람들은.... 어떤 휴대폰을 쓰니??",
"스트레스 해소법으로 '음식 섭취'를 고른 사람들의 지역 분포",
"기분 좋아지는 소비로 '맛잇는 음식 먹기'를 꼽은 사람들의 성별",
"스킨케어 제품 구매 시 '가격'을 중요하게 보는 사람들의 최종 학력",
"월 스킨케어 비용으로 '15만원 이상' 쓰는 헤비 유저",
"'갤럭시 S24' 사용자와 '아이폰 15' 사용자 중 피부 만족도 차이",
"최근 '외식비' 지출이 많은 사람들은 스트레스를 '경제적 문제'로 받을까?",
"가장 기분 좋아지는 소비가 '취미관련 제품 구매'인 사람들의 직업",
"피부 만족도가 '보통' 이하인 사람들은 스킨케어 제품 구매 시 '성분'을 많이 볼까?",
"'업무' 스트레스가 높은 사람들은 '수면'으로 스트레스를 많이 풀어?",
"서울 지역 패널의 최근 주요 지출 항목이 궁금해",
"가장 스트레스 받는 상황으로 '인간관계'를 꼽은 사람들의 월 스킨케어 비용",
"직업이 '자영업'인 사람들은 최근 '배달비' 지출이 많을까?",

    ]
    
    print("--- (API 시뮬레이터) 새 질의를 INSERT하고, 1건씩 파이프라인을 실행합니다... ---")
    for q in test_queries:
        print("\n--- 새 작업 시작 ---")
        insert_and_run_pipeline(q)
