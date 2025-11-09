import re
import redis
from kiwipiepy import Kiwi


class TextNormalizer:
    def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0):
        """텍스트 정규화 클래스"""
        print("✅ TextNormalizer: Redis 연결 시도 중...")
        self.redis = redis.StrictRedis(
            host=redis_host, port=redis_port, db=redis_db, decode_responses=True
        )
        print("✅ TextNormalizer: Redis 연결 성공")

        print("🔧 TextNormalizer: Kiwipiepy 초기화 중...")
        self.kiwi = Kiwi()
        print("✅ TextNormalizer: Kiwipiepy 초기화 완료")

        # Redis 해시 키 (sync_dict_to_redis.py와 동일)
        self.hash_typo = "typo_rules"
        self.hash_synonym = "synonym_rules"

        # 규칙 저장용
        self.typo_patterns = []
        self.synonym_patterns = []

        # 규칙 로드
        self.load_rules_from_redis()

    # -------------------------------
    # 1️⃣ Redis에서 규칙 로드
    # -------------------------------
    def load_rules_from_redis(self):
        print("\n📥 TextNormalizer: Redis에서 규칙 로드 중...")

        typo_rules = self.redis.hgetall(self.hash_typo)
        synonym_rules = self.redis.hgetall(self.hash_synonym)

        print(f"  - Typo 규칙: {len(typo_rules)}개")
        print(f"  - Synonym 규칙: {len(synonym_rules)}개")

        self.typo_patterns = [(k, v) for k, v in typo_rules.items() if k and v and k != v]
        self.synonym_patterns = sorted(
            [(k, v) for k, v in synonym_rules.items() if k and v and k != v],
            key=lambda x: len(x[0]),
            reverse=True
        )

        print(f"  [Typo] 규칙 로드 완료 ({len(self.typo_patterns)}개)")
        print(f"  [Synonym] 규칙 로드 완료 ({len(self.synonym_patterns)}개)")
        print("✅ TextNormalizer: 규칙 로드 완료\n")

    # -------------------------------
    # 2️⃣ 전처리
    # -------------------------------
    def preprocess(self, text: str) -> str:
        text = text.strip()
        text = text.replace("'", "").replace('"', "")
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    # -------------------------------
    # 3️⃣ 치환 (중복/겹침/재적용 방지)
    # -------------------------------
    def apply_replacements(self, text: str, rules, stage_name: str):
        applied = []
        used_rules = set()

        for term_from, term_to in rules:
            if (term_from, term_to) in used_rules:
                continue

            # 패턴 정의: 단어 경계, 부분 매칭 방지
            pattern = re.compile(rf"(?<!\w){re.escape(term_from)}(?!\w)")
            if not pattern.search(text):
                continue

            # 교체 수행
            new_text, count = pattern.subn(term_to, text)
            if count > 0:
                # 🔒 term_to가 포함된 부분은 다시 교체하지 않도록
                if term_to in text:
                    continue

                text = new_text
                applied.append((term_from, term_to))
                used_rules.add((term_from, term_to))

        # 후처리: “이상 이상” / “만원 만원” 같은 중복 제거
        text = re.sub(r"(\b\w+\b)(\s+\1)+", r"\1", text)
        text = re.sub(r"\s{2,}", " ", text).strip()

        if applied:
            print(f"2️⃣ {stage_name} 치환 ({len(applied)}건): {applied[:5]}")
        return text, applied

    # -------------------------------
    # 4️⃣ 형태소 분석 + 문장 복원
    # -------------------------------
    def tokenize_and_normalize(self, text: str):
        sentences = self.kiwi.split_into_sents(text)
        restored_text = " ".join([s.text for s in sentences]).strip()
        tokens = self.kiwi.tokenize(text)
        token_tuples = [(t.form, t.tag) for t in tokens]
        print(f"3️⃣ 토큰화 결과: {token_tuples}")
        return restored_text

    # -------------------------------
    # 5️⃣ 전체 정제 파이프라인
    # -------------------------------
    def normalize(self, text: str, verbose: bool = True):
        """전체 정제 파이프라인"""
        original_text = text
        if verbose:
            print("============================================================")
            print(f"원본 (Raw): {original_text}")
            print("============================================================")

        # 0️⃣ 전처리
        text = self.preprocess(text)
        if verbose:
            print(f"0️⃣ 전처리: {text}")

        # 1️⃣ Typo 교정
        if self.typo_patterns:
            text, _ = self.apply_replacements(text, self.typo_patterns, "Typo")

        # 2️⃣ Synonym 교정
        if self.synonym_patterns:
            text, _ = self.apply_replacements(text, self.synonym_patterns, "Synonym")

        # 3️⃣ 형태소 분석 및 복원
        text = self.tokenize_and_normalize(text)

        if verbose:
            print(f"✅ 최종 결과: {text}")
            print("============================================================")

        return text
