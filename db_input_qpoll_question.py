import pandas as pd
import psycopg2
from datetime import datetime
import re

# --- 1. DB 설정 ---
DB_CONFIG = {
    "host": "34.50.13.135",
    "database": "pre_capstone",
    "user": "pre_capstone",
    "password": "pre_capstone1234!"
}

# --- 2. 파일 및 시트 설정 ---
DATA_FILE = 'qpoll_join_250624.xlsx'

RESPONSE_SHEET_NAME = 'qpoll_join_250624'
METADATA_SHEET_NAME = 'qpoll_20250804151144'

# 헤더(고유번호, 성별...)가 시작되는 행 번호입니다. (3번째 행 -> 2)
RESPONSE_HEADER_ROW = 1 

# 각 데이터에 해당하는 컬럼의 정확한 헤더 이름을 입력하세요.
USER_SN_HEADER = '고유번호'
GENDER_HEADER = '성별'
BIRTHDATE_HEADER = '나이'
REGION_HEADER = '지역'


# --- 4. 설문 정보 시트의 "구조" 설정 (엑셀 파일 형식이 동일하다면 수정할 필요 없음) ---

# 첫 번째 설문 정보가 시작되는 행 번호입니다. (2번째 행 -> 1)
METADATA_POLL_START_ROW = 1 

# 각 설문 정보 사이의 간격입니다. (2행, 4행, 6행... 이므로 2행 간격)
METADATA_POLL_ROW_STEP = 2  

# 설문 제목이 있는 열의 위치입니다. (A열 -> 0)
TITLE_COLUMN_INDEX = 0      

# 첫 번째 보기가 시작되는 열의 위치입니다. (B열 -> 1)
OPTIONS_START_COLUMN_INDEX = 1 

# 최대 몇 개의 보기 컬럼까지 확인할지 설정합니다. (보기6까지 확인)
MAX_OPTIONS_TO_CHECK = 5


def setup_poll_metadata(cursor, poll_title, poll_options):
    """설문 마스터 데이터를 DB에 준비하고 ID 정보를 반환합니다."""
    cursor.execute("SELECT poll_id FROM polls WHERE poll_title = %s;", (poll_title,))
    result = cursor.fetchone()
    if not result:
        cursor.execute("INSERT INTO polls (poll_title) VALUES (%s) RETURNING poll_id;", (poll_title,))
        poll_id = cursor.fetchone()[0]
        for option_text in poll_options:
            cursor.execute("INSERT INTO poll_options (poll_id, option_text) VALUES (%s, %s);", (poll_id, option_text))
    else:
        poll_id = result[0]
    
    cursor.execute("SELECT option_id FROM poll_options WHERE poll_id = %s ORDER BY option_id;", (poll_id,))
    option_ids = [row[0] for row in cursor.fetchall()]
    return poll_id, {str(i + 1): option_id for i, option_id in enumerate(option_ids)}

def main():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("🚀 DB 연결 성공!")

        meta_df = pd.read_excel(DATA_FILE, sheet_name=METADATA_SHEET_NAME, header=None)
        polls_in_file = []
        poll_counter = 1
        for row_idx in range(METADATA_POLL_START_ROW, len(meta_df), METADATA_POLL_ROW_STEP):
            row_data = meta_df.iloc[row_idx]
            poll_title = row_data.iloc[TITLE_COLUMN_INDEX]
            
            if pd.isna(poll_title): continue

            options_end_col = OPTIONS_START_COLUMN_INDEX + MAX_OPTIONS_TO_CHECK
            options = row_data.iloc[OPTIONS_START_COLUMN_INDEX:options_end_col].dropna().tolist()
            
            polls_in_file.append({
                'title': poll_title,
                'options': options,
                'response_column': f'문항{poll_counter}'
            })
            poll_counter += 1
        print(f"✅ 설문 정보 시트에서 {len(polls_in_file)}개의 설문을 확인했습니다.")

        resp_df = pd.read_excel(DATA_FILE, sheet_name=RESPONSE_SHEET_NAME, header=RESPONSE_HEADER_ROW)
        
        for _, user_row in resp_df.iterrows():
            user_sn = str(user_row[USER_SN_HEADER])
            if 'w' not in user_sn: continue

            gender = str(user_row[GENDER_HEADER])
            region = str(user_row[REGION_HEADER])

            # --- 수정된 날짜 처리 로직 시작 ---
            birth_date_string = str(user_row[BIRTHDATE_HEADER])
            birth_date = None
            # 정규식을 사용해 'YYYY년 MM월 DD일' 부분만 추출합니다.
            match = re.search(r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일', birth_date_string)
            if match:
                year, month, day = map(int, match.groups())
                birth_date = datetime(year, month, day).date()
            # --- 수정된 날짜 처리 로직 끝 ---

            sql_upsert_user = """
            INSERT INTO users (user_sn, gender, birth_date, region) VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_sn) DO UPDATE SET
                gender = EXCLUDED.gender, birth_date = EXCLUDED.birth_date, region = EXCLUDED.region;
            """
            cursor.execute(sql_upsert_user, (user_sn, gender, birth_date, region))

            # --- 다중 응답 처리 로직 시작 ---
            for poll_info in polls_in_file:
                poll_id, response_map = setup_poll_metadata(cursor, poll_info['title'], poll_info['options'])
                response_column = poll_info['response_column']

                if response_column in user_row and pd.notna(user_row[response_column]):
                    # 1. 콤마(,)를 기준으로 응답을 분리합니다.
                    raw_responses = str(user_row[response_column])
                    individual_responses = raw_responses.split(',')

                    # 2. 분리된 각 응답을 순회하며 DB에 저장합니다.
                    for res_num_str in individual_responses:
                        clean_res_num = res_num_str.strip()
                        chosen_option_id = response_map.get(clean_res_num)
                        
                        if chosen_option_id:
                            sql_insert_response = """
                            INSERT INTO user_poll_responses (user_sn, poll_id, chosen_option_id)
                            VALUES (%s, %s, %s) ON CONFLICT (user_sn, poll_id, chosen_option_id) DO NOTHING;
                            """
                            cursor.execute(sql_insert_response, (user_sn, poll_id, chosen_option_id))
            # --- 다중 응답 처리 로직 끝 ---


        conn.commit()
        print(f"\n🎉 {len(resp_df)}명의 사용자 정보 및 응답 처리가 완료되었습니다!")

    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()
        print("🔌 데이터베이스 연결을 종료했습니다.")

if __name__ == "__main__":
    main()