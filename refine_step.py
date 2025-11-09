"""
텍스트 정규화 모듈 (Redis + Kiwipiepy)

정규화 아키텍처:
0. _preprocess: 기본 전처리 (특수문자, 이모티콘, 자/모, 공백 정리)
1. PreNormalizer: Regex 기반 사전 정규화 (typo + synonym)
2. TextNormalizer: Redis에서 규칙을 로드하고,
                  Kiwipiepy로 토큰화하여 정규화 파이프라인 제공
"""

import redis
from typing import Dict, List, Tuple
from kiwipiepy import Kiwi
import re

class PreNormalizer:
    """
    토큰화 이전에 오탈자(typo)와 동의어(synonym)를 모두 처리하는 정규화기
    "연쇄 치환"을 방지하기 위해 re.sub()를 사용 (Non-Chaining)
    """
    
    def __init__(self, typo_rules: Dict[str, str], synonym_rules: Dict[str, str]):
        """
        Args:
            typo_rules: {'맛잇는': '맛있는', ...}
            synonym_rules: {'옷쇼핑': '옷/패션 관련 제품 구매하기', ...}
        """
        
        # typo와 synonym 규칙을 합침
        self.rules_map = {**synonym_rules, **typo_rules}
        
        # Regex 패턴 생성
        # 1. 긴 키부터 정렬 (e.g. "맛잇는음식"이 "음식"보다 먼저 매치되도록)
        sorted_keys = sorted(self.rules_map.keys(), key=len, reverse=True)
        
        # 2. 키들을 Regex의 | (OR) 연산자로 결합
        # "단어 경계"(\b)를 제거.
        # "돈 문제로" 같은 조사가 붙는 경우를 처리하기 위함.
        # "만족도" 문제는 DB에 "만족도":"만족도" 규칙을 추가하여 해결 필요.
        try:
            # re.escape()는 키에 포함된 특수문자(., ?, *)를 일반 문자로 처리함
            pattern_str = '|'.join(re.escape(k) for k in sorted_keys)
            self.regex_pattern = re.compile(pattern_str)
            print(f"  [PreNormalizer] 총 {len(sorted_keys)}개 규칙 Regex 컴파일 완료 (단어 경계 \b 없음).")
        
        except re.error as e:
            print(f"❌ [PreNormalizer] Regex 컴파일 실패: {e}")
            print("    일부 키가 너무 길거나 복잡할 수 있습니다.")
            self.regex_pattern = None # 실패 시 비활성화
            
    def _replacer(self, match: re.Match) -> str:
        """re.sub()가 호출할 함수"""
        # 매치된 원본 키 (e.g. "맛잇는음식")
        matched_key = match.group(0) 
        # 맵에서 교체할 값 (e.g. "맛있는 음식")을 찾아 반환
        return self.rules_map.get(matched_key, matched_key)

    def normalize(self, text: str) -> str:
        """
        사전 정규화 (오탈자 + 동의어) 실행
        
        Args:
            text: 입력 텍스트
            
        Returns:
            교정된 텍스트
        """
        if not self.regex_pattern:
            # Regex 컴파일 실패 시 원본 텍스트 반환
            return text
            
        # re.sub()를 사용하여 "연쇄 치환" 없이 단 한 번만 교체
        return self.regex_pattern.sub(self._replacer, text)


class TextNormalizer:
    """Redis 기반 텍스트 정규화 파이프라인 (Kiwipiepy 토큰화 포함)"""
    
    def __init__(
        self,
        redis_host: str = 'localhost',
        redis_port: int = 6379,
        redis_db: int = 0,
        redis_password: str = None
    ):
        """
        Redis에서 규칙을 로드하여 정규화기 초기화
        """
        # Redis 연결
        redis_config = {
            'host': redis_host,
            'port': redis_port,
            'db': redis_db,
            'decode_responses': True
        }
        if redis_password:
            redis_config['password'] = redis_password
        
        self.redis_client = redis.Redis(**redis_config)
        
        # 연결 테스트
        try:
            self.redis_client.ping()
            print("✅ Redis 연결 성공")
        except redis.RedisError as e:
            print(f"❌ Redis 연결 실패: {e}")
            raise
        
        # Kiwipiepy 초기화
        print("🔧 Kiwipiepy 초기화 중...")
        self.kiwi = Kiwi()
        print("✅ Kiwipiepy 초기화 완료")
        
        # 규칙 로드
        self._load_rules()
    
    def _load_rules(self):
        """Redis에서 규칙 로드"""
        print("\n📥 Redis에서 규칙 로드 중...")
        
        # TYPO 규칙 로드
        typo_rules = self.redis_client.hgetall('typo_rules')
        print(f"  - TYPO 규칙: {len(typo_rules)}개")
        
        # SYNONYM 규칙 로드
        synonym_rules = self.redis_client.hgetall('synonym_rules')
        print(f"  - SYNONYM 규칙: {len(synonym_rules)}개")
        
        # 정규화기 초기화
        # 두 규칙을 모두 PreNormalizer에 전달
        self.pre_normalizer = PreNormalizer(typo_rules, synonym_rules)
        
        print("✅ 규칙 로드 완료\n")
    
    def reload_rules(self):
        """규칙 재로드 (Redis 업데이트 후 호출)"""
        print("\n🔄 규칙 재로드 중...")
        self._load_rules()
    
    def _preprocess(self, text: str) -> str:
        """
       0단계: 가장 기본적인 텍스트 전처리 (사전 정규화 전)
        """
        # 1. 양쪽 끝 공백 제거
        text = text.strip()
        
        # 2. '허용 목록' 외 모든 문자 제거 (이모티콘, 특수문자, 자/모음 등)
        # 허용: 한글(가-힣), 영문(a-z, A-Z), 숫자(0-9), 구두점(~/), 공백
        # [^...]: ...을 제외한 모든 문자
        # 허용 목록에서 '?', '.', '!' 제거
        text = re.sub(r'[^가-힣a-zA-Z0-9~/ ]', '', text)
        
        # 3. 중간의 여러 공백을 하나로 합침
        text = re.sub(r'\s+', ' ', text)
        
        return text

    def normalize(self, text: str, verbose: bool = False) -> str:
        """
        텍스트 정규화 (수정된 아키텍처)
        
        0. 기본 전처리 (_preprocess)
        1. 사전 정규화 (typo + synonym "비연쇄" Regex 치환)
        2. Kiwipiepy 토큰화
        3. 최종 문자열 재조합
        
        Args:
            text: 입력 텍스트
            verbose: 단계별 출력 여부
            
        Returns:
            정규화된 텍스트
        """
        if verbose:
            print(f"\n{'='*60}")
            print(f"원본 (Raw): {text}")
            print(f"{'='*60}")
        
        # 0단계: 가장 기본적인 전처리
        text_processed = self._preprocess(text)
        
        if verbose:
            print(f"0️⃣  기본 전처리: {text_processed}")

        # 1단계: 사전 정규화 (Typo + Synonym)
        pre_normalized = self.pre_normalizer.normalize(text_processed)
        
        if verbose:
            print(f"1️⃣  사전 정규화: {pre_normalized}")
        
        # 2단계: Kiwipiepy 토큰화
        tokens = self.kiwi.tokenize(pre_normalized)
        
        if verbose:
            token_list_str = [(token.form, token.tag) for token in tokens]
            print(f"2️⃣  토큰화: {token_list_str}")
        
        # 3단계: 최종 문자열 재조합
        # ' '.join 대신 kiwipiepy의 join 메서드를 사용하여
        # 토큰화 결과에 맞는 올바른 띄어쓰기를 적용합니다.
        final_text = self.kiwi.join(tokens).strip()
        
        if verbose:
            print(f"3️⃣  최종 결과: {final_text}")
            print(f"{'='*60}\n")
        
        return final_text



# 데모 함수들(demo, batch_normalize_demo, interactive_demo)을 삭제하고,
# 이 파일이 모듈로서 import될 수 있도록 정리했습니다.
# 아래의 __name__ == '__main__' 부분은
# 이 스크립트 파일(text_refine_step_fixed_v2.py)을 직접 실행했을 때만
# 작동하는 간단한 테스트 코드입니다.
if __name__ == '__main__':
    
    print("="*60)
    print("TextNormalizer 모듈 테스트")
    print("="*60)
    
    try:
        # 1. 정규화기 초기화 (Redis 연결 및 규칙 로드)
        #    (Redis 서버가 실행 중이어야 합니다)
        normalizer = TextNormalizer(
            redis_host='localhost',
            redis_port=6379,
            redis_db=0
        )
        
        # 2. 테스트 쿼리
        test_queries = [
        "부산 거주자들은 최근 어디에 지출을 가장 많이 했어?",
"아이폰 사용자와 갤럭시 사용자 중 '여행 가기'를 더 선호하는 쪽은?",
"피부 만족도가 '매우 만족'이면서 스킨케어에 '3만원 미만' 쓰는 사람",
"'사무직'이면서 '인간관계'로 스트레스 받는 패널 찾아줘",
"최근 '문화생활'에 지출한 사람들이 가장 기분 좋아지는 소비는?",
"'갤럭시 Z플립' 사용자의 피부 만족도 평균",
"가구소득이 1000만원 이상인 사람들은 스트레스 해소법으로 '운동'을 많이 선택해?",
"스킨케어 제품 구매 시 '성분 및 효과'를 보는 사람들의 성별 분포",
"대구에 사는 50대 기혼 여성 패널",
"자녀가 2명 이상인 패널",
"혼자 사는 (1인 가구) 패널의 직업 분포",
"대학원 재학중인 패널",
"BMW 5시리즈 보유자들은 어디 살아?",
"LG V50 휴대폰을 사용하는 사람",

        ]
        
        print("\n[모듈 테스트 실행]")
        
        # 3. 정규화 실행 (상세 모드 True)
        for query in test_queries:
            result = normalizer.normalize(query, verbose=True)
            print(f"✅ 최종 결과: '{result}'")
            
    except redis.exceptions.ConnectionError as e:
        print(f"❌ Redis 연결 실패. Redis 서버가 실행 중인지 확인하세요.")
        print(f"   오류: {e}")
    except Exception as e:
        print(f"❌ 예기치 않은 오류 발생: {e}")
