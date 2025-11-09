#!/usr/bin/env python3
"""
PostgreSQL dictionary_rules → Redis 동기화
rule_type별로 구분해서 저장 (typo_rules, synonym_rules)
"""

import psycopg2
import redis
from typing import Dict
import sys


# --- 1. 설정 (사용자님 설정) ---
# PostgreSQL 설정
DB_SETTINGS = {
    "dbname": "final",      # 👈 데이터베이스 이름
    "user": "kjw8567",      # 👈 사용자 이름
    "password": "8567",     # 👈 비밀번호
    "host": "localhost",
    "port": "5432"
}

# Redis 설정
REDIS_SETTINGS = {
    "host": "127.0.0.1",    # 또는 "localhost"
    "port": 6379,           # Redis 기본 포트
    "password": None,       # Redis 비밀번호 있으면 입력
    "db": 0,                # 기본 데이터베이스 번호
    "decode_responses": True # 문자열로 자동 디코딩
}
# --- 설정 끝 ---


class DictionarySync:
    """PostgreSQL dictionary_rules를 Redis로 동기화"""
    
    def __init__(self, pg_settings: Dict, redis_settings: Dict):
        self.pg_config = pg_settings
        self.redis_config = redis_settings
        self.pg_conn = None
        self.redis_client = None

    # ------------------------------
    # 1. 연결
    # ------------------------------
    def connect(self):
        """PostgreSQL과 Redis에 연결"""
        try:
            print(f"PostgreSQL 연결 중... ({self.pg_config['host']}:{self.pg_config['port']})")
            self.pg_conn = psycopg2.connect(**self.pg_config)
            print("✅ PostgreSQL 연결 성공")

            print(f"Redis 연결 중... ({self.redis_config['host']}:{self.redis_config['port']})")
            self.redis_client = redis.Redis(**self.redis_config)
            self.redis_client.ping()
            print("✅ Redis 연결 성공")

        except psycopg2.Error as e:
            print(f"❌ PostgreSQL 연결 실패: {e}")
            sys.exit(1)
        except redis.RedisError as e:
            print(f"❌ Redis 연결 실패: {e}")
            sys.exit(1)

    # ------------------------------
    # 2. PostgreSQL 조회
    # ------------------------------
    def fetch_rules_from_postgresql(self) -> Dict[str, list]:
        """PostgreSQL에서 dictionary_rules 조회"""
        print("\n📊 PostgreSQL에서 규칙 조회 중...")
        cursor = self.pg_conn.cursor()

        try:
            cursor.execute("""
                SELECT rule_type, term_from, term_to 
                FROM dictionary_rules
                ORDER BY rule_type, term_from
            """)
            rules = {'TYPO': [], 'SYNONYM': []}
            for rule_type, term_from, term_to in cursor.fetchall():
                if rule_type in rules:
                    rules[rule_type].append((term_from, term_to))
                else:
                    print(f"⚠️  알 수 없는 rule_type: {rule_type} (무시)")
            print(f"  - TYPO 규칙: {len(rules['TYPO'])}개")
            print(f"  - SYNONYM 규칙: {len(rules['SYNONYM'])}개")
            print(f"  - 총 규칙: {len(rules['TYPO']) + len(rules['SYNONYM'])}개")
            return rules

        except psycopg2.Error as e:
            print(f"❌ PostgreSQL 조회 실패: {e}")
            sys.exit(1)
        finally:
            cursor.close()

    # ------------------------------
    # 3. Redis 기존 데이터 삭제
    # ------------------------------
    def clear_redis_rules(self):
        """Redis의 기존 규칙 삭제"""
        print("\n🗑️  Redis 기존 규칙 삭제 중...")
        try:
            deleted_typo = self.redis_client.delete('typo_rules')
            deleted_synonym = self.redis_client.delete('synonym_rules')
            print(f"  - typo_rules 삭제: {'삭제됨' if deleted_typo else '없었음'}")
            print(f"  - synonym_rules 삭제: {'삭제됨' if deleted_synonym else '없었음'}")
        except redis.RedisError as e:
            print(f"❌ Redis 삭제 실패: {e}")
            sys.exit(1)

    # ------------------------------
    # 4. Redis에 규칙 저장 (루프 방식)
    # ------------------------------
    def save_rules_to_redis(self, rules: Dict[str, list]):
        """규칙을 Redis에 저장"""
        print("\n💾 Redis에 규칙 저장 중...")

        try:
            # TYPO 규칙 저장
            if rules['TYPO']:
                print(f"  - typo_rules 저장 중... ({len(rules['TYPO'])}개)")
                pipeline = self.redis_client.pipeline(transaction=False)
                for term_from, term_to in rules['TYPO']:
                    pipeline.hset('typo_rules', term_from, term_to)
                pipeline.execute()
                print("    ✅ typo_rules 저장 완료")

            # SYNONYM 규칙 저장
            if rules['SYNONYM']:
                print(f"  - synonym_rules 저장 중... ({len(rules['SYNONYM'])}개)")
                pipeline = self.redis_client.pipeline(transaction=False)
                for term_from, term_to in rules['SYNONYM']:
                    pipeline.hset('synonym_rules', term_from, term_to)
                pipeline.execute()
                print("    ✅ synonym_rules 저장 완료")

        except redis.RedisError as e:
            print(f"❌ Redis 저장 실패: {e}")
            sys.exit(1)

    # ------------------------------
    # 5. 검증
    # ------------------------------
    def verify_redis_data(self):
        """Redis에 저장된 데이터 검증"""
        print("\n🔍 Redis 저장 데이터 검증 중...")

        try:
            typo_count = self.redis_client.hlen('typo_rules')
            synonym_count = self.redis_client.hlen('synonym_rules')

            print(f"  - typo_rules: {typo_count}개")
            print(f"  - synonym_rules: {synonym_count}개")

            print("\n📋 샘플 데이터:")

            print("\n  [TYPO 규칙 샘플]")
            typo_sample = list(self.redis_client.hscan_iter('typo_rules', count=3))
            if not typo_sample:
                print("    (데이터 없음)")
            for i, (term_from, term_to) in enumerate(typo_sample[:3], 1):
                print(f"    {i}. '{term_from}' → '{term_to}'")

            print("\n  [SYNONYM 규칙 샘플]")
            synonym_sample = list(self.redis_client.hscan_iter('synonym_rules', count=3))
            if not synonym_sample:
                print("    (데이터 없음)")
            for i, (term_from, term_to) in enumerate(synonym_sample[:3], 1):
                print(f"    {i}. '{term_from}' → '{term_to}'")

            print("\n✅ 검증 완료!")

        except redis.RedisError as e:
            print(f"❌ Redis 검증 실패: {e}")
            sys.exit(1)

    # ------------------------------
    # 6. 전체 실행
    # ------------------------------
    def sync(self, clear_existing: bool = True, verify: bool = True):
        """전체 동기화 프로세스"""
        print("=" * 60)
        print("PostgreSQL → Redis 동기화 시작")
        print("=" * 60)

        self.connect()
        rules = self.fetch_rules_from_postgresql()

        if clear_existing:
            self.clear_redis_rules()

        self.save_rules_to_redis(rules)

        if verify:
            self.verify_redis_data()

        print("\n" + "=" * 60)
        print("✅ 동기화 완료!")
        print("=" * 60)

    # ------------------------------
    # 7. 종료
    # ------------------------------
    def close(self):
        """연결 종료"""
        if self.pg_conn:
            self.pg_conn.close()
            print("\n🔌 PostgreSQL 연결 종료")
        if self.redis_client:
            self.redis_client.close()
            print("🔌 Redis 연결 종료")


# ------------------------------
# 메인 실행부
# ------------------------------
def main():
    sync = DictionarySync(DB_SETTINGS, REDIS_SETTINGS)

    try:
        sync.sync(clear_existing=True, verify=True)
    finally:
        sync.close()


if __name__ == '__main__':
    main()
