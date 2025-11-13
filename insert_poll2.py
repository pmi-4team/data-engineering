import pandas as pd
import psycopg2
import re
import os  # os.path.basename을 사용하기 위해 추가

# --- 사용자 설정 ---
DB_CONFIG = {
   "dbname": "f",
   "user": "",
   "password": "7",
   "host": "",
   "port": "5432"
}

# ❗️ 데이터를 읽어올 엑셀 파일 경로 (이 파일을 바꿔가며 실행)
EXCEL_FILE_PATH = 'C:/Users/ecopl/Desktop/paneldata/Quickpoll/qpoll_join_250723.xlsx'

# ------------------
# ❗️ [수정됨] 예외 처리 목록
#
# [참고]
# 대부분의 파일은 '총참여자수'/'CNT' 헤더를 기반으로 보기를 자동 인식합니다.
# 이 자동 인식이 실패하는 비정상적인 엑셀 파일이 있을 경우에만
# {ID: 개수} 형태로 예외를 추가합니다.
#
# 현재 모든 파일이 정상 형식이므로, 비워둡니다.
POLL_MAX_OPTIONS = {
    # 6: 7, (자동 로직이 더 정확하므로 삭제함)
} 
# ------------------

def clean_value(value):
    """NaN 등을 None으로 변환하는 함수"""
    if pd.isna(value):
        return None
    val_str = str(value).strip()
    return val_str if val_str else None

# --- 🛠️ 함수: 문항 제목 기반 매핑 ---
def get_mapping_from_db_and_excel(cur, file_path):
    """
    DB에서 polls, poll_options 정보를 읽고, 엑셀 문항 제목을 기준으로
    (poll_id, 엑셀 선택지 번호) -> option_id 매핑 정보를 구축합니다.
    """
    print(f"\n-> [1/3] 파일 처리 시작: {os.path.basename(file_path)}")
    print("  - DB 및 엑셀 정보를 사용하여 매핑 정보를 구축합니다...")
    
    # 1. DB의 polls 정보 가져오기: (정규화된 poll_title) -> poll_id 맵 구축
    cur.execute("SELECT poll_id, poll_title FROM polls;")
    db_polls = cur.fetchall()
    
    db_poll_title_map = {}
    for poll_id, poll_title in db_polls:
        # DB 문항 제목 정규화 (공백 제거)
        normalized_title = poll_title.strip().replace(' ', '')
        db_poll_title_map[normalized_title] = poll_id
    
    print(f"  - DB에 {len(db_polls)}개의 설문 문항이 존재합니다. (제목 기반 매칭 예정)")
    
    # 2. 엑셀의 문항/선택지 정보 읽기
    try:
        df_polls = pd.read_excel(file_path, sheet_name=1, header=None)
    except IndexError:
        raise ValueError("엑셀 파일에 두 번째 시트(설문 정의)가 존재하지 않습니다.")

    # 3. 매핑 구축을 위한 딕셔너리 초기화
    db_option_text_map = {} 
    option_id_map = {}
    
    # DB에서 poll_options 전체를 가져와 (poll_id, 정규화된 option_text) -> option_id 맵을 만듭니다.
    cur.execute("SELECT poll_id, option_text, option_id FROM poll_options;")
    for poll_id, option_text, option_id in cur.fetchall():
        normalized_text = option_text.strip().replace(' ', '')
        db_option_text_map[(poll_id, normalized_text)] = option_id

    processed_poll_count = 0
    mapped_poll_ids = []

    # 엑셀 시트 구조를 순회하며 매핑 구축
    for index in range(1, len(df_polls), 2):
        poll_row = df_polls.iloc[index]
        header_row = df_polls.iloc[index - 1] # 헤더 행(0, 2, 4...)도 함께 참조
            
        excel_poll_title = clean_value(poll_row.iloc[0])
        
        if not excel_poll_title:
            continue
            
        excel_normalized_title = excel_poll_title.replace(' ', '')
        poll_id = db_poll_title_map.get(excel_normalized_title)

        if not poll_id:
             print(f"  ⚠️ 경고: 엑셀 문항 ('{excel_poll_title}')에 해당하는 Poll ID가 DB에 존재하지 않습니다. 이 문항의 응답은 누락됩니다.")
             continue

        mapped_poll_ids.append(poll_id)
        processed_poll_count += 1
        
        # 🌟 수동 필터링 값 가져오기
        max_options = POLL_MAX_OPTIONS.get(poll_id)
        
        # 🟢 통계 컬럼 위치를 찾아 데이터 범위 제한 (기본 필터링)
        option_start_col_idx = 1 # B열
        option_end_col_idx = len(df_polls.columns) 
        
        # 헤더 행(header_row)을 검사하여 통계 컬럼 위치 찾기 (총참여자수/CNT 전까지)
        for col_idx, col_header_value in enumerate(header_row):
            if col_idx < option_start_col_idx: continue

            header_text = clean_value(col_header_value)
            
            if header_text and ('총참여자수' in header_text or 'CNT' in header_text):
                option_end_col_idx = col_idx
                break
        
        # 🌟 수동 필터링 적용 (특정 poll_id의 선택지 개수를 강제)
        if max_options is not None:
             target_end_idx = option_start_col_idx + max_options
             # 수동 설정값이 자동 인식값보다 클 경우에만 강제로 늘림
             if target_end_idx > option_end_col_idx:
                 print(f"  ℹ️ 정보: Poll ID {poll_id}에 대해 선택지 끝 인덱스를 {option_end_col_idx}에서 {target_end_idx}로 강제 조정했습니다.")
                 option_end_col_idx = target_end_idx
            
        clean_option_data = poll_row.iloc[option_start_col_idx : option_end_col_idx]
        
        # --- 🟢 필터링 끝 ---
        
        option_num = 1
        for option_text in clean_option_data:
            option_text = clean_value(option_text)
            
            # 엑셀 정의 시트에 빈 칸이 있거나, 4자리 미만의 숫자가 있는 경우 건너뜁니다.
            if not option_text or (option_text.isdigit() and len(option_text) < 4):
                continue
            
            excel_normalized_text = option_text.replace(' ', '')
            db_option_id = db_option_text_map.get((poll_id, excel_normalized_text))
            
            if db_option_id:
                option_id_map[(poll_id, option_num)] = db_option_id
            else:
                # 경고 메시지에 엑셀 선택지 번호(option_num)도 포함
                print(f"  ⚠️ 경고: Poll ID {poll_id}의 선택지 #{option_num} '{option_text}'가 DB에 존재하지 않습니다. 응답 누락.")

            option_num += 1
            
    print(f"\n  - 총 {processed_poll_count}개 문항의 선택지 매핑 구축 완료.")
    return option_id_map, mapped_poll_ids

# --- user_poll_responses 처리 함수 (변경 없음) ---
def process_user_responses(cur, file_path, option_id_map, poll_ids_in_order):
    """
    첫 번째 시트에서 사용자 응답을 읽고, DB에 존재하는 사용자의 응답만 user_poll_responses 테이블에 저장합니다.
    """
    print("\n-> [2/3] 사용자 설문 응답 정보를 처리합니다...")

    cur.execute("SELECT user_sn FROM users;")
    valid_users = {row[0] for row in cur.fetchall()}
    print(f"  - DB에서 {len(valid_users)}명의 유효 사용자 정보를 확인했습니다.")

    df_responses = pd.read_excel(file_path, sheet_name=0, header=1, dtype=str)
    
    poll_columns = sorted([col for col in df_responses.columns if str(col).startswith('문항')])
    
    if len(poll_columns) != len(poll_ids_in_order):
        print(f"  ⚠️ 경고: 엑셀 응답 열({len(poll_columns)}개)과 매칭된 문항 수({len(poll_ids_in_order)}개)가 다릅니다. 매칭 오류가 발생할 수 있습니다.")
    
    processed_count = 0
    for index, row in df_responses.iterrows():
        user_sn = clean_value(row.get('고유번호'))
        
        if not user_sn or user_sn not in valid_users:
            continue
            
        for i, poll_col in enumerate(poll_columns):
            if i >= len(poll_ids_in_order):
                break 

            poll_id = poll_ids_in_order[i]
            
            response_val_str = clean_value(row.get(poll_col))
            if not response_val_str:
                continue 
            
            response_numbers = [item.strip() for item in response_val_str.split(',')]
            
            for num_str in response_numbers:
                try:
                    chosen_option_num = int(float(num_str))
                except (ValueError, TypeError):
                    continue 

                chosen_option_id = option_id_map.get((poll_id, chosen_option_num))
                
                if chosen_option_id:
                    cur.execute(
                        """
                        INSERT INTO user_poll_responses (chosen_option_id, poll_id, user_sn)
                        VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING; 
                        """,
                        (chosen_option_id, poll_id, user_sn)
                    )
                    # cur.rowcount는 0(중복/충돌) 또는 1(신규 삽입)을 반환
                    processed_count += cur.rowcount 
    
    print(f"  - 총 {processed_count}개의 새로운 유효 응답을 DB에 저장했습니다.")

# --- 메인 실행 로직 (변경 없음) ---
if __name__ == "__main__":
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("데이터베이스에 성공적으로 연결되었습니다.\n")
        
        option_map, generated_poll_ids = get_mapping_from_db_and_excel(cur, EXCEL_FILE_PATH)
        
        process_user_responses(cur, EXCEL_FILE_PATH, option_map, generated_poll_ids)
        
        conn.commit()
        print("\n" + "="*50)
        print("모든 설문 관련 정보가 성공적으로 처리 및 커밋되었습니다.")
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
