import pandas as pd
import psycopg2

# --- 설정 ---
DB_CONFIG = {
    "host": "34.50.13.135",
    "database": "pre_capstone",
    "user": "pre_capstone",
    "password": "pre_capstone1234!"
}
FILE_NAME = 'welcome_2nd.xlsx'
DATA_SHEET = 'data'

def main():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("🚀 DB 연결 성공!")

        cursor.execute("SELECT user_sn FROM users;")
        existing_users = {row[0] for row in cursor.fetchall()}
        print(f"✅ DB에서 {len(existing_users)}명의 기존 사용자를 확인했습니다.")

        df = pd.read_excel(FILE_NAME, sheet_name=DATA_SHEET)
        
        inserted_count = 0
        skipped_users = 0

        for index, row in df.iterrows():
            user_sn = str(row['mb_sn'])

            if user_sn not in existing_users:
                skipped_users += 1
                continue

            for question_id in df.columns:
                # --- 수정된 부분 시작 ---
                # 'mb_sn'과 'Q8' 컬럼은 건너뜁니다.
                if question_id.upper() in ['MB_SN', 'Q8']:
                    continue
                # --- 수정된 부분 끝 ---
                
                answer_value = row[question_id]
                
                if pd.isna(answer_value):
                    continue

                answers = str(answer_value).split(',')
                
                for single_answer in answers:
                    clean_answer = single_answer.strip()
                    if clean_answer:
                        # 답변이 숫자 형태(예: '3.0')이면 정수형 문자열('3')로 변환
                        try:
                            clean_answer = str(int(float(clean_answer)))
                        except ValueError:
                            pass # 숫자로 변환할 수 없으면 그냥 사용
                        
                        sql_insert = """
                        INSERT INTO user_profile_answers (user_sn, question_id, answer_value)
                        VALUES (%s, %s, %s);
                        """
                        cursor.execute(sql_insert, (user_sn, question_id.upper(), clean_answer))
                        inserted_count += 1
        
        conn.commit()
        print("\n--- 작업 완료 ---")
        print(f"🎉 총 {inserted_count}개의 프로필 응답이 DB에 삽입되었습니다.")
        print(f"⚠️ users 테이블에 없어 건너뛴 사용자 수: {skipped_users}명")

    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    main()