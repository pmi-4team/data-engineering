import re
import redis
from kiwipiepy import Kiwi


class TextNormalizer:
    def __init__(self, redis_host="localhost", redis_port=6379):
        print("✅ TextNormalizer: Redis 연결 시도 중...")
        self.redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)
        print("✅ TextNormalizer: Redis 연결 성공")

        print("🔧 TextNormalizer: Kiwipiepy 초기화 중...")
        self.kiwi = Kiwi()
        print("✅ TextNormalizer: Kiwipiepy 초기화 완료")

        # ✅ Redis에서 규칙 로드
        self.typo_rules = self.load_rules_from_redis("TYPO")
        self.synonym_rules = self.load_rules_from_redis("SYNONYM")
        print("✅ TextNormalizer: 규칙 로드 완료\n")

    # -----------------------------
    # Redis 규칙 로드 (자동 감지)
    # -----------------------------
    def load_rules_from_redis(self, rule_type):
        key_map = {
            "TYPO": ["typo_rules", "dictionary_rules:TYPO"],
            "SYNONYM": ["synonym_rules", "dictionary_rules:SYNONYM"],
        }

        redis_key = None
        for k in key_map.get(rule_type, []):
            if self.redis_client.exists(k):
                redis_key = k
                break

        if not redis_key:
            redis_key = f"{rule_type.lower()}_rules"

        keys = self.redis_client.hkeys(redis_key)
        rules = []
        for k in keys:
            v = self.redis_client.hget(redis_key, k)
            if k and v:
                rules.append((k.strip(), v.strip()))

        print(f"  [{rule_type}] 규칙 로드 완료 ({len(rules)}개) from '{redis_key}'")
        return rules

    # -----------------------------
    # 텍스트 정규화 수행
    # -----------------------------
    def normalize(self, text: str, verbose=False):
        print(f"\n[정제 시작] 원본: {text}")
        print("=" * 60)

        # 0️⃣ 기본 전처리
        text = self.preprocess_text(text)
        print(f"0️⃣ 전처리: {text}")

        # 1️⃣ 오탈자 교정
        text, typo_applied = self.apply_replacements(text, self.typo_rules, "Typo")

        # 2️⃣ 동의어 교정
        text, syn_applied = self.apply_replacements(text, self.synonym_rules, "Synonym")

        # 3️⃣ 토큰화
        tokens = [(t.form, t.tag) for t in self.kiwi.tokenize(text)]
        print(f"3️⃣ 토큰화 결과: {tokens}")

        # 4️⃣ 후처리
        text = self.postprocess_text(text)
        print(f"✅ 최종 결과: {text}")

        print("=" * 60)
        return text

    # -----------------------------
    # 전처리
    # -----------------------------
    def preprocess_text(self, text: str):
        text = text.strip()
        text = re.sub(r"\s+", " ", text)
        text = re.sub(r"[‘’“”]", "'", text)
        text = re.sub(r"['\"]", "", text)
        return text

    # -----------------------------
    # 교체 로직 (긴 문자열 우선 + 단어단위 일치)
    # -----------------------------
    def apply_replacements(self, text: str, rules, stage_name: str):
        applied = []
        replaced_spans = []
        sorted_rules = sorted(rules, key=lambda x: len(x[0]), reverse=True)

        for term_from, term_to in sorted_rules:
            if not term_from or not term_to:
                continue
            if len(term_from) < 2:
                continue

            # ✅ 단어 단위 매칭만 허용 (부분 일치 방지)
            pattern = re.compile(
                rf"(?<![가-힣A-Za-z0-9]){re.escape(term_from)}(?![가-힣A-Za-z0-9])"
            )

            new_text = text
            offset = 0
            for m in pattern.finditer(text):
                start, end = m.span()
                if any(s <= start < e or s < end <= e for s, e in replaced_spans):
                    continue  # 이미 교체된 부분은 무시

                replacement = term_to
                new_text = new_text[: start + offset] + replacement + new_text[end + offset :]
                offset += len(replacement) - (end - start)
                replaced_spans.append((start + offset, start + offset + len(replacement)))
                applied.append((term_from, term_to))

            text = new_text

        if applied:
            print(f"2️⃣ {stage_name} 치환 ({len(applied)}건): {applied[:5]}")
        return text, applied

    # -----------------------------
    # 후처리: 중복 제거, 공백 정리
    # -----------------------------
    def postprocess_text(self, text: str):
        text = re.sub(r"\b(\w+)( \1\b)+", r"\1", text)  # 같은 단어 반복 제거
        text = re.sub(r"\s+", " ", text).strip()
        text = re.sub(r"([가-힣]+)\s+\1", r"\1", text)
        return text
