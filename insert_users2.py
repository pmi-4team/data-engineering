import pandas as pd
import psycopg2
from datetime import datetime

# (사용자 설정 및 함수 정의는 이전과 동일)
# --- 사용자 설정 ---
DB_CONFIG = {
    "dbname": "?",
    "user": "?",
    "password": "!",
    "host": "?",
    "port": "5432"
}

EXCEL_FILE_PATH_BASE = 'C:/Users/ecopl/Desktop/qpoll 데이터/필수/qpoll_join_250624.xlsx'
EXCEL_FILE_PATH_FILTER = 'C:/Users/ecopl/Desktop/qpoll 데이터/필수/welcome_2nd.xlsx'

USER_ID_COLUMN_NAME = 'mb_sn'
# ------------------

# --- 함수 정의 ---
def parse_birthdate_from_excel(birth_date_str):
    """엑셀에서 읽은 날짜 문자열을 date 객체로 변환합니다."""
    # clean_value는 외부에서 미리 호출
    if not birth_date_str:
        return None
    try:
        if '년' in str(birth_date_str):
            # '월 일'과 같은 불완전한 텍스트를 걸러내기 위한 추가 검사
            if '월 일' in birth_date_str:
                return None
            date_part = birth_date_str.split('(')[0].strip()
            return pd.to_datetime(date_part, format='%Y년 %m월 %d일').date()
        return pd.to_datetime(birth_date_str).date()
    except (ValueError, TypeError):
        return None

def clean_value(value):
    """NaN, 빈 값 등을 None으로 변환하는 함수"""
    if pd.isna(value):
        return None
    val_str = str(value).strip().lower()
    if val_str in ['nan', '[null]', 'null', '']:
        return None
    return str(value).strip()

def process_users_after_cleaning(cur, file_path_base, file_path_filter):
    """BASE 파일에서 유효한 날짜를 가진 행을 먼저 찾고, FILTER 파일과 비교 후 저장합니다."""
    print("\n-> [1/4] 기본 사용자 정보 파일을 읽습니다...")
    df_base = pd.read_excel(file_path_base, header=1, dtype=str)
    df_base.rename(columns={'고유번호': 'mb_sn', '나이': 'birth_date', '지역': 'region', '성별': 'gender'}, inplace=True)
    df_base.columns = [str(col).lower() for col in df_base.columns]

    # --- [핵심 변경 1] ---
    # 먼저 birth_date 컬럼을 파싱하여 새로운 'parsed_date' 컬럼을 만듭니다.
    # 파싱에 실패하면 이 컬럼 값은 NaT(Not a Time) 또는 None이 됩니다.
    print("\n-> [2/4] 기본 파일에서 유효한 데이터(정상 날짜 형식 등)를 필터링합니다...")
    df_base['parsed_date'] = df_base['birth_date'].apply(parse_birthdate_from_excel)
    
    # 필수 컬럼과 새로 만든 파싱된 날짜 컬럼을 기준으로 null 값이 없는 행만 필터링합니다.
    required_cols = ['mb_sn', 'parsed_date', 'region', 'gender']
    clean_df_base = df_base.dropna(subset=required_cols)
    print(f"   - 총 {len(df_base)}개 중 {len(clean_df_base)}개의 유효한 행을 찾았습니다.")


    print("\n-> [3/4] 필터링용 사용자 정보 파일을 읽습니다...")
    df_filter = pd.read_excel(file_path_filter, header=0, dtype=str)
    df_filter_ids = df_filter[['mb_sn']].copy()
    df_filter_ids.columns = [str(col).lower() for col in df_filter_ids.columns]

    print("\n-> [4/4] 두 파일의 공통 사용자 정보를 필터링하고 DB에 저장합니다...")
    merge_key = USER_ID_COLUMN_NAME.lower()
    
    final_df = pd.merge(clean_df_base, df_filter_ids, on=merge_key, how='inner')
    
    processed_count = 0
    for index, row in final_df.iterrows():
        user_sn = clean_value(row.get(merge_key))
        # 이미 파싱해둔 날짜 객체를 사용합니다. 다시 파싱할 필요가 없습니다.
        birth_date_obj = row.get('parsed_date') 
        region = clean_value(row.get('region'))
        gender = clean_value(row.get('gender'))
        
        cur.execute(
            """
            INSERT INTO users (user_sn, gender, birth_date, region)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_sn) DO NOTHING;
            """,
            (user_sn, gender, birth_date_obj, region)
        )
        processed_count += 1
    
    print(f"\n-> 'users' 테이블 처리 완료.")
    print(f"   - 총 {len(final_df)}개 공통 유효 레코드 중 {processed_count}개 저장.")

# (메인 실행 로직은 동일)
# --- 메인 실행 로직 ---
if __name__ == "__main__":
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("데이터베이스에 성공적으로 연결되었습니다.\n")
        
        process_users_after_cleaning(cur, EXCEL_FILE_PATH_BASE, EXCEL_FILE_PATH_FILTER)
        
        conn.commit()
        print("\n" + "="*50)
        print("모든 사용자 정보가 성공적으로 처리 및 커밋되었습니다.")
        print("="*50)

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"\n오류가 발생하여 작업을 취소하고 롤백했습니다.\n에러: {e}")

    finally:
        if conn:
            cur.close()
            conn.close()
            print("데이터베이스 연결을 종료했습니다.")
