import re
import redis


class TextNormalizer:
    def __init__(self, redis_host="localhost", redis_port=6379):
        print("✅ TextNormalizer: Redis 연결 시도 중...")
        self.redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)
        print("✅ TextNormalizer: Redis 연결 성공")

        # Redis에서 규칙 로드
        self.typo_rules = self.load_rules_from_redis("TYPO")
        self.synonym_rules = self.load_rules_from_redis("SYNONYM")
        print("✅ TextNormalizer: 규칙 로드 완료\n")

        # ✅ 복합어 접미사 목록
        self.compound_suffixes = ['도', '률', '율', '적', '성', '감', '력', '능']

        # ✅ 동사/형용사 어미 확장 세트
        self.verb_suffixes = ['하', '해', '했', '하고', '하는', '했다', '하며', '하게', '하여']

    # -----------------------------------------------------
    def load_rules_from_redis(self, rule_type):
        """Redis에서 규칙 로드"""
        key_map = {
            "TYPO": ["typo_rules", "dictionary_rules:TYPO"],
            "SYNONYM": ["synonym_rules", "dictionary_rules:SYNONYM"],
        }

        redis_key = None
        for k in key_map.get(rule_type, []):
            if self.redis_client.exists(k):
                redis_key = k
                print(f"  🔍 [{rule_type}] Redis 키 발견: '{k}'")
                break

        if not redis_key:
            redis_key = f"{rule_type.lower()}_rules"

        keys = self.redis_client.hkeys(redis_key)
        rules = []
        for k in keys:
            v = self.redis_client.hget(redis_key, k)
            if k and v:
                k = re.sub(r"\s+", " ", k.strip())
                v = re.sub(r"\s+", " ", v.strip())
                rules.append((k, v))

        print(f"  ✅ [{rule_type}] 규칙 로드 완료: {len(rules)}개 from '{redis_key}'")
        return rules

    # -----------------------------------------------------
    def normalize(self, text: str, verbose=False):
        """문자열 기반 정제 전체 파이프라인"""
        print(f"\n[정제 시작] 원본: {text}")
        print("=" * 60)

        text = self.preprocess_text(text)
        print(f"0️⃣ 전처리: {text}")

        # 오탈자 교정
        text, typo_applied = self.apply_replacements(text, self.typo_rules, "Typo")
        if typo_applied:
            print(f"1️⃣ Typo 교정 후: {text}")
        else:
            print(f"1️⃣ Typo 교정: (변화 없음)")

        # 동의어 표준화
        text, syn_applied = self.apply_replacements(text, self.synonym_rules, "Synonym")
        if syn_applied:
            print(f"2️⃣ Synonym 표준화 후: {text}")
        else:
            print(f"2️⃣ Synonym 표준화: (변화 없음)")

        # 후처리
        text = self.postprocess_text(text)
        print(f"✅ 최종 결과: {text}")
        print("=" * 60)

        return text

    # -----------------------------------------------------
    def preprocess_text(self, text: str):
        """기본 전처리: 공백/기호 정리"""
        text = text.strip()
        text = re.sub(r"\s+", " ", text)
        text = re.sub(r"[''""]", "'", text)
        text = re.sub(r"['\"]+", "", text)
        text = re.sub(r"[^가-힣a-zA-Z0-9\s\.\!\?\~\-\(\)]", " ", text)  # ✅ 괄호 허용
        text = re.sub(r"([.!?~])\1+", r"\1", text)
        text = re.sub(r"([.!?]){2,}$", r"\1", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    # -----------------------------------------------------
    def apply_replacements(self, text: str, rules, stage_name: str):
        """문자열 기반 교체 로직 (부분 겹침 허용 버전)"""
        applied = []
        replaced_spans = []

        if not rules:
            return text, applied

        # 긴 단어 우선 교체
        sorted_rules = sorted(rules, key=lambda x: len(x[0]), reverse=True)
        print(f"\n  🔍 [{stage_name}] 규칙 적용 시작 (총 {len(sorted_rules)}개 규칙)")

        for term_from, term_to in sorted_rules:
            if not term_from or not term_to or len(term_from) < 2:
                continue

            escaped = re.escape(term_from).replace(r"\ ", r"\s+")

            # 조사 및 어미 허용
            if stage_name.lower() == "typo":
                pattern = re.compile(
                    rf"(?<![가-힣A-Za-z0-9]){escaped}"
                    rf"(?=[^가-힣A-Za-z0-9]|[에에서로도만은는이가]?|"
                    rf"[가-힣]*({'|'.join(self.verb_suffixes)})?|$)"
                )
            else:
                pattern = re.compile(
                    rf"(?<![가-힣A-Za-z0-9]){escaped}"
                    rf"(?=[^A-Za-z0-9]|[에에서로도만은는이가]?|"
                    rf"[가-힣]*({'|'.join(self.verb_suffixes)})?|$)"
                )

            matches = list(pattern.finditer(text))

            if len(applied) < 5 and matches:
                print(f"    ✓ 매칭: '{term_from}' → '{term_to}' ({len(matches)}개 매칭)")

            for m in matches:
                start, end = m.span()

                # ✅ 완전히 포함된 경우만 skip (부분 겹침은 허용)
                if any(s <= start and end <= e for s, e in replaced_spans):
                    continue

                if '~' in text[max(0, start - 3): min(len(text), end + 3)]:
                    continue

                matched_text = text[start:end]
                after_match = text[start:start + len(term_to)]
                if after_match == term_to:
                    continue

                # 복합어 보호 (예: "감성적" → "감성" 방지)
                if stage_name.lower() == "synonym":
                    next_char_pos = end
                    if next_char_pos < len(text):
                        next_char = text[next_char_pos]
                        if next_char in self.compound_suffixes:
                            continue

                # 교체 실행
                text = text[:start] + term_to + text[end:]
                replaced_spans.append((start, start + len(term_to)))
                applied.append((term_from, term_to))
                break

        if applied:
            print(f"  📊 [{stage_name}] 적용 완료: {len(applied)}개 규칙 적용됨")
        else:
            print(f"  ℹ️  [{stage_name}] 적용된 규칙 없음")

        return text, applied

    # -----------------------------------------------------
    def postprocess_text(self, text: str):
        """후처리: 반복 단어/공백 정리"""
        text = re.sub(r"\b(\w+)( \1\b)+", r"\1", text)
        text = re.sub(r"([가-힣]+)\s+\1\b", r"\1", text)
        text = re.sub(r"(이다|입니다|었다|았다|있다|없다|하다|한다)(\1)+", r"\1", text)
        text = re.sub(r"(\S+\([^)]+\))\s+\1", r"\1", text)
        text = re.sub(r"(\S+(?:\s+\S+){0,3})\s+\1", r"\1", text)
        text = re.sub(r"(\b[가-힣a-zA-Z0-9\s]+)\s+\1\b", r"\1", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text
