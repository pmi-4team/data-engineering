# config.py

# --- 1. DB 설정 ---
DB_SETTINGS = {
    "dbname": "",
    "user": "",
    "password": "",
    "host": "",
    "port": "5432"
}
# --- 2. Redis 설정 ---
# REDIS_SETTINGS = {
#     "host": "",  # 또는 "localhost"
#     "port": 6379,          # Redis 기본 포트
#     "password": "",     
#     "db": 0,               # 기본 데이터베이스 번호
#     "decode_responses": True
# }

# --- 3. 텍스트 정규화(V3) 설정 ---
# sync_dict_to_redis.py가 저장하는 Redis 키와 일치해야 합니다.
TYPO_RULES_KEY = "typo_rules"
SYNONYM_RULES_KEY = "synonym_rules"

# --- 4. PII (개인정보) 패턴 ---
# (참고: 이 PII 패턴은 현재 새 파이프라인에서는 사용되지 않습니다.)
# (새 TextNormalizer의 _preprocess 함수가 이보다 더 강력하게 처리합니다.)
PII_PATTERNS = [
    ('[CARD]', r'\b(\d{4})[\s.-]?(\d{4})[\s.-]?(\d{4})[\s.-]?(\d{4})\b'),
    ('[JUMIN]', r'\b(\d{6})[\s.-](\d{7})\b'),
    ('[PHONE]', r'\b(010)[\s.-]?(\d{4})[\s.-]?(\d{4})\b'),
    ('[EMAIL]', r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
]

# (이 PII 사전도 현재 새 파이프라인에서는 사용되지 않습니다.)
PII_DICTIONARIES = {
    '[ADDR]': ['서울', '부산', '대구', '인천', '광주', '대전', '울산', '세종'],
    '[NAME]': ['김민준', '이서준', '박도윤', '최하준', '정이준', '강지호'] 
}
