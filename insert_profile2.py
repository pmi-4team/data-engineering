import pandas as pd
import psycopg2
import re

# --- 사용자 설정 ---
DB_CONFIG = {
    "dbname": "?",
    "user": "?",
    "password": "?",
    "host": "?",
    "port": "5432"
}
# welcome_2nd.xlsx 파일 경로
EXCEL_FILE_PATH = '?'
# ------------------

def clean_value(value):
    """NaN 등을 None으로 변환하는 함수"""
    if pd.isna(value):
        return None
    val_str = str(value).strip()
    return val_str if val_str else None

def process_profile_questions_and_options(cur, file_path):
    """두 번째 시트에서 프로필 질문을 읽어 DB에 저장하고, 옵션 맵을 반환."""
    print("-> [1/2] 프로필 질문 및 선택지 정보를 처리합니다...")
    
    try:
        df_profile = pd.read_excel(file_path, sheet_name=1, header=None)
    except IndexError:
        raise ValueError("엑셀 파일에 두 번째 시트(label)가 존재하지 않습니다.")

    qa_map = {}
    current_question_id = None
    
    for index, row in df_profile.iterrows():
        col_a_val = clean_value(row.iloc[0])
        col_b_val = clean_value(row.iloc[1])
        col_c_val = clean_value(row.iloc[2])

        if not col_b_val:
            continue

        if col_a_val and col_a_val.upper().startswith('Q'):
            current_question_id = col_a_val.upper()
            question_text = col_b_val
            question_type = col_c_val.upper() if col_c_val else 'TEXT'
            
            cur.execute(
                """
                INSERT INTO profile_questions (question_id, question_text, question_type) 
                VALUES (%s, %s, %s) ON CONFLICT (question_id) DO UPDATE
                SET question_text = EXCLUDED.question_text, question_type = EXCLUDED.question_type;
                """,
                (current_question_id, question_text, question_type)
            )
            print(f"   - [Question] '{question_text}' ({current_question_id}, Type: {question_type}) 저장 완료.")
            qa_map[current_question_id] = {}

        elif current_question_id and col_a_val and col_a_val.isdigit():
            option_code = col_a_val
            option_text = col_b_val
            
            # --- [수정됨] profile_options 테이블 저장 로직 제거 ---
            # cur.execute(
            #     """
            #     INSERT INTO profile_options (question_id, option_code, option_text) 
            #     VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;
            #     """,
            #     (current_question_id, option_code, option_text)
            # )
            # ----------------------------------------------------
            
            # qa_map은 user_profile_answers 처리를 위해 여전히 필요하므로 남겨둡니다.
            qa_map[current_question_id][option_code] = option_text
            
    print("   - 모든 선택지 (In-Memory Map) 생성 완료.")
    return qa_map

def process_user_profile_answers(cur, file_path, qa_map):
    """첫 번째 시트에서 사용자 응답을 읽고, DB에 존재하는 사용자의 응답만 저장합니다."""
    print("\n-> [2/2] 사용자 프로필 응답 정보를 처리합니다...")

    cur.execute("SELECT user_sn FROM users;")
    valid_users = {row[0] for row in cur.fetchall()}
    print(f"   - DB에서 {len(valid_users)}명의 유효 사용자 정보를 확인했습니다.")

    df_answers = pd.read_excel(file_path, sheet_name=0, header=0, dtype=str)
    
    processed_count = 0
    for index, row in df_answers.iterrows():
        user_sn = clean_value(row.get('mb_sn'))
        
        if not user_sn or user_sn not in valid_users:
            continue
            
        for col_name_raw in row.index:
            col_name = str(col_name_raw).strip()
            question_id = col_name.upper()
            
            if not question_id.startswith('Q') or question_id not in qa_map:
                continue

            response_codes_str = clean_value(row.get(col_name_raw))
            if not response_codes_str:
                continue

            response_codes = [item.strip() for item in response_codes_str.split(',') if item.strip()]
            
            for code in response_codes:
                # qa_map에서 코드에 해당하는 텍스트를 찾습니다.
                answer_value = qa_map[question_id].get(code)
                
                if not answer_value:
                    # Q5_1_ETC 처럼 맵에 없는 주관식 응답은 코드(원본값) 자체를 사용합니다.
                    answer_value = code
                
                if answer_value and answer_value.strip():
                    # 최종 answer_value를 DB에 저장하기 전에 소문자로 변환합니다.
                    final_answer_value = answer_value.lower()
                    
                    cur.execute(
                        """
                        INSERT INTO user_profile_answers (user_sn, question_id, answer_value)
                        VALUES (%s, %s, %s) ON CONFLICT DO NOTHING; 
                        """,
                        (user_sn, question_id, final_answer_value) # 소문자로 변환된 값으로 저장
                    )
                    processed_count += 1
    
    print(f"   - 총 {processed_count}개의 유효한 사용자 응답을 저장했습니다.")
    
# --- 메인 실행 로직 ---
if __name__ == "__main__":
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("데이터베이스에 성공적으로 연결되었습니다.\n")
        
        # 1단계: 프로필 질문/선택지 정의 읽고 DB 저장
        qa_map = process_profile_questions_and_options(cur, EXCEL_FILE_PATH)
        
        # 2단계: 사용자 응답 읽고 DB 저장
        process_user_profile_answers(cur, EXCEL_FILE_PATH, qa_map)
        
        conn.commit()
        print("\n" + "="*50)
        print("모든 프로필 정보가 성공적으로 처리 및 커밋되었습니다.")
        print("="*50)

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"\n❌ 오류가 발생하여 작업을 취소하고 롤백했습니다.\n에러: {e}")

    finally:
        if conn:
            cur.close()
            conn.close()
            print("데이터베이스 연결을 종료했습니다.")