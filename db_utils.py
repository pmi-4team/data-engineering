# db_utils.py
"""
PostgreSQL 데이터베이스 관련 유틸리티 함수 모음
"""

import psycopg2
import sys
from config import DB_SETTINGS

def get_db_connection():
    """
    PostgreSQL 데이터베이스 연결을 생성하고 반환합니다.
    
    Returns:
        psycopg2.connection: DB 연결 객체 (실패 시 None)
    """
    try:
        conn = psycopg2.connect(**DB_SETTINGS)
        return conn
    except psycopg2.Error as e:
        print(f"❌ DB 연결 실패: {e}", file=sys.stderr)
        return None


def find_new_job(cursor):
    """
    query_logs 테이블에서 is_normalized_hit가 NULL인 작업 1개를 조회합니다.
    
    Args:
        cursor: psycopg2 커서 객체
        
    Returns:
        tuple: (log_id, raw_query) 또는 None
    """
    sql_select = """
        SELECT log_id, raw_query
        FROM query_logs
        WHERE is_normalized_hit IS NULL
        ORDER BY timestamp ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED;
    """
    
    try:
        cursor.execute(sql_select)
        result = cursor.fetchone()
        return result
    except psycopg2.Error as e:
        print(f"❌ 작업 조회 실패: {e}", file=sys.stderr)
        return None


def find_normalization_key(cursor, normalization_key):
    """
    query_normalization 테이블에서 정제 키를 검색합니다.
    
    Args:
        cursor: psycopg2 커서 객체
        normalization_key (str): 5단계 정제를 거친 표준화된 키
        
    Returns:
        tuple: (normalization_id,) 또는 None
    """
    sql_check = """
        SELECT normalization_id
        FROM query_normalization
        WHERE normalization_key = %s;
    """
    
    try:
        cursor.execute(sql_check, (normalization_key,))
        result = cursor.fetchone()
        return result
    except psycopg2.Error as e:
        print(f"❌ 정제 키 검색 실패: {e}", file=sys.stderr)
        return None


def update_normalization_hit_count(cursor, normalization_id):
    """
    query_normalization 테이블의 hit_count를 1 증가시킵니다.
    
    Args:
        cursor: psycopg2 커서 객체
        normalization_id (int): 업데이트할 normalization_id
    """
    sql_update_hit = """
        UPDATE query_normalization
        SET hit_count = hit_count + 1
        WHERE normalization_id = %s;
    """
    
    try:
        cursor.execute(sql_update_hit, (normalization_id,))
    except psycopg2.Error as e:
        print(f"❌ hit_count 업데이트 실패: {e}", file=sys.stderr)
        raise


def insert_new_normalization_entry(cursor, normalization_key):
    """
    query_normalization 테이블에 새로운 정제 키를 삽입합니다.
    
    Args:
        cursor: psycopg2 커서 객체
        normalization_key (str): 5단계 정제를 거친 표준화된 키
        
    Returns:
        int: 생성된 normalization_id
    """
    sql_insert = """
        INSERT INTO query_normalization (normalization_key)
        VALUES (%s)
        RETURNING normalization_id;
    """
    
    try:
        cursor.execute(sql_insert, (normalization_key,))
        normalization_id = cursor.fetchone()[0]
        return normalization_id
    except psycopg2.Error as e:
        print(f"❌ query_normalization 삽입 실패: {e}", file=sys.stderr)
        raise


def update_log_status(cursor, log_id, is_normalized_hit, normalization_id):
    """
    query_logs 테이블의 정제 상태를 업데이트합니다.
    
    Args:
        cursor: psycopg2 커서 객체
        log_id (int): 업데이트할 로그 ID
        is_normalized_hit (bool): 정제 완료 여부
        normalization_id (int): 매핑할 normalization_id
    """
    sql_update_log = """
        UPDATE query_logs
        SET is_normalized_hit = %s, normalization_id = %s
        WHERE log_id = %s;
    """
    
    try:
        cursor.execute(sql_update_log, (is_normalized_hit, normalization_id, log_id))
    except psycopg2.Error as e:
        print(f"❌ query_logs 업데이트 실패: {e}", file=sys.stderr)
        raise


def get_normalization_stats(cursor):
    """
    정제 통계를 조회합니다.
    
    Args:
        cursor: psycopg2 커서 객체
        
    Returns:
        dict: 통계 정보
    """
    sql_stats = """
        SELECT 
            COUNT(*) as total_normalizations,
            SUM(hit_count) as total_hits,
            AVG(hit_count) as avg_hits
        FROM query_normalization;
    """
    
    try:
        cursor.execute(sql_stats)
        result = cursor.fetchone()
        return {
            'total_normalizations': result[0],
            'total_hits': result[1],
            'avg_hits': float(result[2]) if result[2] else 0
        }
    except psycopg2.Error as e:
        print(f"❌ 통계 조회 실패: {e}", file=sys.stderr)
        return None


def get_top_queries(cursor, limit=10):
    """
    가장 많이 조회된 정제 키 목록을 반환합니다.
    
    Args:
        cursor: psycopg2 커서 객체
        limit (int): 반환할 최대 개수
        
    Returns:
        list: [(normalization_key, hit_count), ...]
    """
    sql_top = """
        SELECT normalization_key, hit_count
        FROM query_normalization
        ORDER BY hit_count DESC
        LIMIT %s;
    """
    
    try:
        cursor.execute(sql_top, (limit,))
        results = cursor.fetchall()
        return results
    except psycopg2.Error as e:
        print(f"❌ 인기 질의 조회 실패: {e}", file=sys.stderr)
        return []
