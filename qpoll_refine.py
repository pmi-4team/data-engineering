import pandas as pd
import sys
import requests
import time
import json
import pprint # 📌 추가: 딕셔너리를 예쁘게 출력하기 위해 import

# --- ⚙️ 사용자 설정 ---
INPUT_FILE = "qpoll5.xlsx"      # 1. 원본 엑셀 파일명
# 📌 수정: 딕셔너리 맵을 저장할 파일
OUTPUT_FILE = "job_map_generated.py" # 2. 저장할 텍스트 파일명 (.py로 변경)
# --- -----------------

# 📌 1차 분류기(캐시)로 사용할 맵
typo_map = {
    '취준생': '취업준비생',
    '취업준비중': '취업준비생',
    '준비중': '취업준비생',
    '취업준비': '취업준비생',
    '시험준비중': '취업준비생',
    '대학원 준비생': '취업준비생',
    '경찰준비생': '취업준비생',
    '재수생': '취업준비생',
    '무직/취업준비': '취업준비생',
    '취준': '취업준비생',
    '간호조무사 준비중': '취업준비생'
}
# --- -----------------


# --- 🤖 Gemini API 설정 ---
API_KEY = "" # 1. 사용자가 입력한 API 키
# 📌 수정: 올바른 API 모델 이름으로 변경
model_name = "gemini-2.0-flash" 
API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={API_KEY}"


# ... (SYSTEM_PROMPT는 변경 없음) ...
SYSTEM_PROMPT = """
당신은 한국어 직업 분류 전문가입니다.
사용자가 입력한 직업명을 보고, 다음 16가지 카테고리 중 하나로 정확하게 분류해야 합니다.
괄호 안의 예시를 참고하여 가장 적절한 카테고리를 선택하세요.

---
전문직 (의사, 간호사, 변호사, 회계사, 예술가, 종교인, 엔지니어, 프로그래머, 기술사 등)
교직 (교수, 교사, 강사 등)
경영/관리직 (사장, 대기업 간부, 고위 공무원 등)
사무직 (기업체 차장 이하 사무직 종사자, 공무원 등)
자영업 (제조업, 건설업, 도소매업, 운수업, 무역업, 서비스업 경영)
판매직 (보험판매, 세일즈맨, 도/소매업 직원, 부동산 판매, 행상, 노점상 등)
서비스직 (미용, 통신, 안내, 요식업 직원 등)
생산/노무직 (차량운전자, 현장직, 생산직 등)
기능직 (기술직, 제빵업, 목수, 전기공, 정비사, 배관공 등)
농업/임업/축산업/광업/수산업
임대업
중/고등학생
대학생/대학원생
전업주부
퇴직/연금생활자
무직
---

응답은 오직 16개의 카테고리 이름 중 하나여야 합니다. (예: "전문직 (의사, 간호사, 변호사, 회계사, 예술가, 종교인, 엔지니어, 프로그래머, 기술사 등)")
다른 설명, 인사, "분류:" 같은 접두사를 절대 붙이지 마세요.
만약 '무직', '학생' 등 명확한 항목이 있다면, 해당 카테고리(예: "무직", "대학생/대학원생")로 정확히 분류하세요.
분류가 정말 불가능하면 "무직"으로 응답하세요.
"""
# --- -----------------

NEW_COLUMN_NAME = "직업_분류"
UNMAPPED_VALUE = "분류_불가(API_실패)"


# ... ('get_classification' 함수는 변경 없음) ...
def get_classification(job_title):
    """
    Gemini API를 호출하여 단일 직업명을 분류하고, 지수 백오프로 재시도합니다.
    """
    
    # 빈 문자열이나 공백은 API 호출 없이 '무직'으로 분류
    if not job_title:
        return "무직"
        
    payload = {
        "contents": [{"parts": [{"text": job_title}]}],
        "systemInstruction": {
            "parts": [{"text": SYSTEM_PROMPT}]
        }
    }
    
    headers = {'Content-Type': 'application/json'}
    max_retries = 5
    delay = 1.0  # 1초 딜레이로 시작

    for attempt in range(max_retries):
        try:
            # 'requests.post'를 사용하여 동기식으로 API 호출 (타임아웃 60초)
            response = requests.post(API_URL, json=payload, headers=headers, timeout=60)

            if response.status_code == 200:
                result = response.json()
                classification = result.get('candidates', [{}])[0] \
                                       .get('content', {}) \
                                       .get('parts', [{}])[0] \
                                       .get('text', '').strip()
                
                if not classification:
                    return "무직" 
                
                return classification
            
            else:
                # 429 (Too Many Requests) 또는 5xx 서버 에러 시 재시도
                if response.status_code == 429 or response.status_code >= 500:
                    print(f"  [API] '{job_title}' 분류 중 {response.status_code} 에러. {delay:.1f}초 후 재시도... ({attempt + 1}/{max_retries})")
                    time.sleep(delay)  # 'time.sleep()' 사용
                    delay *= 2.0
                else:
                    print(f"  [API] '{job_title}' 분류 실패 (상태 코드: {response.status_code})")
                    return UNMAPPED_VALUE
        
        # 'requests.exceptions.Timeout'으로 변경
        except requests.exceptions.Timeout:
            print(f"  [API] '{job_title}' 분류 중 타임아웃. {delay:.1f}초 후 재시도... ({attempt + 1}/{max_retries})")
            time.sleep(delay)
            delay *= 2.0
        
        except Exception as e:
            print(f"  [API] '{job_title}' 처리 중 예외 발생: {e}")
            time.sleep(delay)
            delay *= 2.0

    print(f"  [API] '{job_title}' 분류 최종 실패.")
    return UNMAPPED_VALUE


# 'classify_jobs_concurrently' 함수 제거


def main():
    try:
        # 1. 파일을 (.xlsx) 받아옴
        print(f"'{INPUT_FILE}' 파일을 읽는 중...")
        # 📌 수정: .head(50) 제거 -> 전체 파일 로드
        df = pd.read_excel(INPUT_FILE)
        print("파일 읽기 완료 (전체 파일 로드).")
        
    except FileNotFoundError:
        print(f"오류: '{INPUT_FILE}' 파일을 찾을 수 없습니다.")
        print("스크립트와 같은 폴더에 엑셀 파일이 있는지 확인하세요.")
        sys.exit(1)
    except Exception as e:
        print(f"엑셀 파일 읽기 중 오류 발생: {e}")
        sys.exit(1)

    # ... (첫 번째 컬럼 이름 가져오는 로직) ...
    if df.empty or len(df.columns) == 0:
        print("오류: 엑셀 파일이 비어있거나 컬럼이 없습니다.")
        sys.exit(1)
    # 📌 수정: '첫 번째 컬럼' -> '모든 컬럼'으로 로그 변경
    print(f"엑셀의 *모든 컬럼*에서 고유 항목을 수집합니다.")


    # 3. 매핑 딕셔너리 생성
    print(f"모든 컬럼의 고유 항목으로 'typo_map' 생성을 시작합니다...")
    
    # 📌 수정: 맵을 만들기 위해 *모든* 컬럼의 *고유한(unique)* 값만 추출
    # 1. df.values.ravel() -> 모든 값을 1D 배열로 폄
    # 2. pd.Series(...) -> pandas 기능(astype, strip, unique)을 쓰기 위해 변환
    all_values_series = pd.Series(df.values.ravel())
    cleaned_series = all_values_series.fillna('').astype(str).str.strip()
    unique_jobs = cleaned_series.unique()
    
    total_unique_jobs = len(unique_jobs)
    print(f"총 {total_unique_jobs}개의 *고유* 직업을 분류합니다...")
    
    start_time = time.time()
    # 📌 수정: 최종 맵을 저장할 딕셔너리 생성
    final_job_map = {} 
    
    # 📌 수정: for 루프 로직 변경
    # 고유한 직업 리스트(unique_jobs)를 순회
    for i, job in enumerate(unique_jobs):
        
        # 1. typo_map(캐시)에서 먼저 검색
        mapped_val = typo_map.get(job)
        
        if mapped_val:
            # 2. 맵에 있으면: API 호출 안 함
            result = mapped_val
            log_source = "MAP"
        else:
            # 3. 맵에 없으면: API 호출
            result = get_classification(job)
            log_source = "API"
        
        # 📌 수정: 결과를 최종 딕셔너리에 저장
        final_job_map[job] = result
        
        # 5개 처리할 때마다 진행 상황 출력
        if (i + 1) % 5 == 0 or (i + 1) == total_unique_jobs:
            print(f"  ...진행 중: {i + 1} / {total_unique_jobs} 완료 (원본: '{job}' -> 분류: '{result}' [출처: {log_source}])")
    
    end_time = time.time()
    print(f"맵 생성 완료. (소요 시간: {end_time - start_time:.2f}초)")

    # 📌 수정: 4. 텍스트 파일 형태로 저장 (DataFrame이 아닌 딕셔너리 저장)
    try:
        print(f"결과(딕셔너리)를 '{OUTPUT_FILE}' 파일로 저장 중...")
        
        # 딕셔너리를 pprint.pformat을 사용해 예쁜 문자열로 변환
        output_string = "# -*- coding: utf-8 -*-\n"
        output_string += "# 생성된 직업 분류 맵\n"
        output_string += "generated_job_map = {\n"
        
        # 📌 수정: API 실패 항목을 걸러내고, 나머지를 가나다 순으로 정렬
        sorted_items = sorted(
            (k, v) for k, v in final_job_map.items() if v != UNMAPPED_VALUE
        )
        
        # 정렬된 항목들을 딕셔너리처럼 포매팅하여 추가
        # (pprint.pformat(dict(sorted_items))는 딕셔너리라 순서 보장이 안 될 수 있어 수동 포매팅)
        for k, v in sorted_items:
            output_string += f"    '{k}': '{v}',\n"

        output_string += "\n}\n"
        
        # -----------------------------------------------------
        # API 실패 항목이 있다면 파일 끝에 따로 주석으로 추가
        failed_items = [
            k for k, v in final_job_map.items() if v == UNMAPPED_VALUE
        ]
        if failed_items:
            output_string += "\n# --- 분류 실패 항목 (API 오류) ---\n"
            output_string += "# " + "\n# ".join(failed_items)
            output_string += "\n"
        # -----------------------------------------------------


        # 새 파일을 열어 문자열을 씀
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            f.write(output_string)
        
        print("-" * 30)
        print(f"🎉 작업 완료! '{OUTPUT_FILE}' 파일을 확인하세요.")
        if failed_items:
             print(f"  (경고: {len(failed_items)}개의 항목이 API 분류에 실패했습니다.)")
        print("-" * 30)
        
    except Exception as e:
        print(f"파일 저장 중 오류 발생: {e}")


if __name__ == "__main__":
    main()

