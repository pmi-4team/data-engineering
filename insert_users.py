import pandas as pd
import psycopg2
from datetime import datetime

# --- 사용자 설정 ---
DB_CONFIG = {
    "dbname": "final",
    "user": "kjw8567",
    "password": "8567",
    "host": "localhost",
    "port": "5432"
}

EXCEL_FILE_PATH_BASE = 'C:/Users/ecopl/Desktop/paneldata/Quickpoll/qpoll_join_250106.xlsx'
EXCEL_FILE_PATH_FILTER = 'C:/Users/ecopl/Desktop/paneldata/Welcome/welcome_2nd.xlsx'
EXCEL_FILE_PATH_DETAIL = 'C:/Users/ecopl/Desktop/paneldata/Welcome/welcome_1st.xlsx'

USER_ID_COLUMN_NAME = 'mb_sn'


# --- 함수 정의 ---
def parse_birthdate_from_excel(birth_date_str):
    """엑셀에서 읽은 날짜 문자열을 date 객체로 변환합니다."""
    if not birth_date_str:
        return None
    try:
        if '년' in str(birth_date_str):
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


def process_users_after_cleaning(cur, file_path_base, file_path_filter, file_path_detail):
    """BASE, FILTER, DETAIL 3개 파일의 공통 사용자를 찾아 DB에 저장합니다."""
    
    print("\n-> [1/5] 기본 사용자 정보 파일(qpoll)을 읽습니다...")
    df_base = pd.read_excel(file_path_base, header=1, dtype=str)
    df_base.rename(columns={'고유번호': 'mb_sn', '나이': 'birth_date', '지역': 'region', '성별': 'gender'}, inplace=True)
    df_base.columns = [str(col).lower() for col in df_base.columns]

    print("\n-> [2/5] 기본 파일에서 유효한 데이터(정상 날짜 형식 등)를 필터링합니다...")
    df_base['parsed_date'] = df_base['birth_date'].apply(parse_birthdate_from_excel)
    required_cols = ['mb_sn', 'parsed_date', 'region', 'gender']
    clean_df_base = df_base.dropna(subset=required_cols)
    print(f"     - 총 {len(df_base)}개 중 {len(clean_df_base)}개의 유효한 행을 찾았습니다.")

    print("\n-> [3/5] 필터링용 사용자 정보 파일(Welcome_2nd)을 읽습니다...")
    df_filter = pd.read_excel(file_path_filter, header=0, dtype=str)
    df_filter_ids = df_filter[['mb_sn']].copy()
    df_filter_ids.columns = [str(col).lower() for col in df_filter_ids.columns]

    print("\n-> [4/5] 상세 지역 정보 파일(Welcome_1st)을 읽습니다...")
    df_detail = pd.read_excel(file_path_detail, header=0, dtype=str)
    if 'Q12_2' not in df_detail.columns:
        print("     - [오류] Welcome_1st 파일에 'Q12_2' 컬럼이 없습니다. 스크립트를 중단합니다.")
        return False
        
    df_detail_data = df_detail[['mb_sn', 'Q12_2']].copy()
    df_detail_data.rename(columns={'Q12_2': 'region_detail'}, inplace=True)
    df_detail_data.columns = [str(col).lower() for col in df_detail_data.columns]
    df_detail_data = df_detail_data.dropna(subset=['region_detail'])
    print(f"     - {len(df_detail_data)}개의 유효한 상세 지역 정보를 찾았습니다.")

    print("\n-> [5/5] 3개 파일의 공통 사용자 정보를 필터링하고 DB에 저장합니다...")
    merge_key = USER_ID_COLUMN_NAME.lower()
    temp_df = pd.merge(clean_df_base, df_filter_ids, on=merge_key, how='inner')
    final_df = pd.merge(temp_df, df_detail_data, on=merge_key, how='inner')
    
    processed_count = 0
    inserted_count = 0
    updated_count = 0
    inserted_users = []

    for index, row in final_df.iterrows():
        user_sn = clean_value(row.get(merge_key))
        birth_date_obj = row.get('parsed_date') 
        region = clean_value(row.get('region'))
        gender = clean_value(row.get('gender'))
        region_detail = clean_value(row.get('region_detail'))

        # 🔹 기존 사용자 여부 확인
        cur.execute("SELECT 1 FROM users WHERE user_sn = %s;", (user_sn,))
        exists = cur.fetchone()

        cur.execute(
            """
            INSERT INTO users (user_sn, gender, birth_date, region, region_detail)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (user_sn) DO UPDATE SET
                gender = EXCLUDED.gender,
                birth_date = EXCLUDED.birth_date,
                region = EXCLUDED.region,
                region_detail = EXCLUDED.region_detail;
            """,
            (user_sn, gender, birth_date_obj, region, region_detail)
        )

        processed_count += 1
        if exists:
            updated_count += 1
        else:
            inserted_count += 1
            inserted_users.append(user_sn)
    
    print(f"\n-> 'users' 테이블 처리 완료.")
    print(f"     - 공통 유효 레코드 총 {len(final_df)}개 중 {processed_count}개 저장.")
    print(f"     - 새로 추가된 사용자: {inserted_count}명")
    print(f"     - 기존 사용자 업데이트: {updated_count}명")

    # 🔹 새로 추가된 사용자 미리보기 (앞 10명)
    if inserted_users:
        preview_count = min(len(inserted_users), 10)
        print(f"\n🆕 이번 실행에서 새로 추가된 사용자 {preview_count}명 (일부 미리보기):")
        for u in inserted_users[:preview_count]:
            print(f"   - {u}")

    return True


# --- 메인 실행 로직 ---
if __name__ == "__main__":
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("데이터베이스에 성공적으로 연결되었습니다.\n")
        
        success = process_users_after_cleaning(
            cur, 
            EXCEL_FILE_PATH_BASE, 
            EXCEL_FILE_PATH_FILTER, 
            EXCEL_FILE_PATH_DETAIL
        )
        
        if success:
            conn.commit()
            print("\n" + "="*50)
            print("모든 사용자 정보가 성공적으로 처리 및 커밋되었습니다.")
            print("="*50)

            # ✅ 전체 사용자 수 출력
            cur.execute("SELECT COUNT(*) FROM users;")
            total_count = cur.fetchone()[0]
            print(f"\n✅ 현재 DB(users) 테이블에 저장된 총 사용자 수: {total_count}명")

        else:
            raise Exception("데이터 처리 중 오류가 발생하여 커밋하지 않았습니다.")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"\n오류가 발생하여 작업을 취소하고 롤백했습니다.\n에러: {e}")

    finally:
        if conn:
            cur.close()
            conn.close()
            print("데이터베이스 연결을 종료했습니다.")
