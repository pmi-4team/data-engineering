import pandas as pd
import psycopg2
import re

# --- 설정 ---
DB_CONFIG = {
    "host": "34.50.13.135",
    "database": "pre_capstone",
    "user": "pre_capstone",
    "password": "pre_capstone1234!"
}
FILE_NAME = 'welcome_2nd.xlsx' 
METADATA_SHEET = 'label'

def main():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("🚀 DB 연결 성공!")
        
        df = pd.read_excel(FILE_NAME, sheet_name=METADATA_SHEET, header=None)
        
        current_question_id = None
        
        for index, row in df.iloc[1:].iterrows(): # 헤더 행 건너뛰기
            col_a_val = str(row.iloc[0]).strip() # A열 값
            col_b_val = str(row.iloc[1]).strip() # B열 값
            col_c_val = str(row.iloc[2]).strip() # C열 값
            
            # A열이 'Q'로 시작하면 질문 행
            if col_a_val.upper().startswith('Q'):
                current_question_id = col_a_val
                sql_question = """
                INSERT INTO profile_questions (question_id, question_text, question_type)
                VALUES (%s, %s, %s) ON CONFLICT (question_id) DO UPDATE SET
                    question_text = EXCLUDED.question_text, question_type = EXCLUDED.question_type;
                """
                cursor.execute(sql_question, (current_question_id, col_b_val, col_c_val))
            
            # A열이 숫자인 경우 보기 행으로 판단
            elif col_a_val.isdigit():
                if current_question_id:
                    option_code = col_a_val
                    option_text = col_b_val
                    
                    sql_option = """
                    INSERT INTO profile_options (question_id, option_code, option_text)
                    VALUES (%s, %s, %s) ON CONFLICT (question_id, option_code) DO UPDATE SET
                        option_text = EXCLUDED.option_text;
                    """
                    cursor.execute(sql_option, (current_question_id, option_code, option_text))

        conn.commit()
        print(f"🎉 '{METADATA_SHEET}' 시트의 프로필 정보가 DB에 성공적으로 준비되었습니다!")

    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    main()