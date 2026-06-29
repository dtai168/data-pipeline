import subprocess
import sys

import pymysql

STARROCKS_HOST = "127.0.0.1"
STARROCKS_PORT = 9030
STARROCKS_USER = "root"
STARROCKS_PASS = ""

TRINO_POD = "trino-coordinator-544985768-psxrg"  # kubectl get pods -n trino
TRINO_NS = "trino"


def trino_query(sql):
    """Chạy query qua Trino (kubectl exec) -> trả list of string rows."""
    cmd = [
        "kubectl", "exec", "-n", TRINO_NS, TRINO_POD,
        "--", "trino", "--server", "localhost:8080",
        "--catalog", "iceberg", "--schema", "chatbot_v2",
        "--execute", sql
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        # Filter out WARNING lines
        err = "\n".join(l for l in result.stderr.split("\n") if "WARNING" not in l and l.strip())
        if err.strip():
            print(f"  Trino ERROR: {err.strip()}")
        return []
    lines = []
    for l in result.stdout.strip().split("\n"):
        l = l.strip()
        if not l:
            continue
        # Remove all quote types: ASCII " and Unicode “ ”
        l = l.replace('"', '').replace('“', '').replace('”', '')
        lines.append(l)
    return lines


def starrocks_exec(conn, sql, params=None):
    """Execute SQL trên StarRocks."""
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params)
        conn.commit()
        return cursor.rowcount
    except Exception as e:
        print(f"  StarRocks ERROR: {e}")
        conn.rollback()
        return 0


def etl_daily_chat_stats(conn):
    """ETL: gold_chat_stats_daily - Tổng hợp chat theo ngày."""
    print("\n=== ETL: gold_chat_stats_daily ===")

    # date_parse vì created_time là varchar "2024-07-02 15:40:45"
    rows = trino_query("""
        SELECT
            CAST(date_parse(created_time, '%Y-%m-%d %H:%i:%s') AS DATE) AS log_date,
            COUNT(1) AS total_messages,
            COUNT(DISTINCT sender_id) AS unique_users
        FROM processed_logs
        WHERE created_time IS NOT NULL AND created_time != ''
        GROUP BY CAST(date_parse(created_time, '%Y-%m-%d %H:%i:%s') AS DATE)
        ORDER BY log_date
    """)

    if not rows:
        print("  Khong co du lieu tu Trino")
        return

    print(f"  Got {len(rows)} rows from Trino")

    # Query user_id per day cho bitmap
    for row in rows:
        parts = row.split(",")
        if len(parts) < 3:
            continue
        log_date = parts[0].strip()
        total_msg = int(parts[1].strip())
        n_users = int(parts[2].strip())

        # Lay danh sach user_id de build bitmap
        user_rows = trino_query(f"""
            SELECT DISTINCT sender_id FROM processed_logs
            WHERE CAST(date_parse(created_time, '%Y-%m-%d %H:%i:%s') AS DATE) = DATE '{log_date}'
              AND sender_id IS NOT NULL AND sender_id != ''
        """)

        for uid in user_rows:
            uid = uid.replace('"', '').replace('“', '').replace('”', '').strip()
            starrocks_exec(conn,
                "INSERT INTO gold_db.gold_chat_stats_daily (log_date, total_messages, unique_users) VALUES (%s, %s, bitmap_hash(%s))",
                (log_date, total_msg, uid)
            )

        print(f"  {log_date}: {total_msg} msgs, {len(user_rows)} users -> OK")


def etl_fallback_stats(conn):
    """ETL: gold_fallback_stats_daily - Ti le fallback theo intent."""
    print("\n=== ETL: gold_fallback_stats_daily ===")

    rows = trino_query("""
        SELECT
            CAST(date_parse(created_time, '%Y-%m-%d %H:%i:%s') AS DATE) AS log_date,
            final_intent AS intent_group,
            COUNT(1) AS total_queries,
            SUM(CASE WHEN final_intent = 'fallback' THEN 1 ELSE 0 END) AS fallback_queries
        FROM processed_logs
        WHERE created_time IS NOT NULL AND created_time != ''
        GROUP BY CAST(date_parse(created_time, '%Y-%m-%d %H:%i:%s') AS DATE), final_intent
        ORDER BY log_date, intent_group
    """)

    if not rows:
        print("  Khong co du lieu tu Trino")
        return

    print(f"  Got {len(rows)} rows from Trino")

    for row in rows:
        parts = row.split(",")
        if len(parts) < 4:
            continue
        log_date = parts[0].strip()
        intent_group = parts[1].strip()
        total_queries = int(parts[2].strip())
        fallback_queries = int(parts[3].strip())

        starrocks_exec(conn,
            "INSERT INTO gold_db.gold_fallback_stats_daily (log_date, intent_group, total_queries, fallback_queries) VALUES (%s, %s, %s, %s)",
            (log_date, intent_group, total_queries, fallback_queries)
        )
        print(f"  {log_date} | {intent_group}: {total_queries} total, {fallback_queries} fallback -> OK")


def verify(conn):
    """Kiem tra du lieu Gold tables."""
    print("\n=== VERIFY: Gold Tables ===")
    cursor = conn.cursor()

    for table in ["gold_chat_stats_daily", "gold_fallback_stats_daily"]:
        cursor.execute(f"SELECT count(*) FROM gold_db.{table}")
        count = cursor.fetchone()[0]
        print(f"\n  {table}: {count} rows")
        cursor.execute(f"SELECT * FROM gold_db.{table} ORDER BY 1 LIMIT 10")
        cols = [d[0] for d in cursor.description]
        print(f"    {cols}")
        for row in cursor.fetchall():
            print(f"    {row}")


if __name__ == "__main__":
    print("StarRocks Gold ETL")
    print(f"StarRocks: {STARROCKS_HOST}:{STARROCKS_PORT}")
    print(f"Trino pod: {TRINO_POD}")

    conn = pymysql.connect(
        host=STARROCKS_HOST, port=STARROCKS_PORT,
        user=STARROCKS_USER, password=STARROCKS_PASS,
        connect_timeout=10
    )

    try:
        etl_daily_chat_stats(conn)
        etl_fallback_stats(conn)
        verify(conn)
    finally:
        conn.close()

    print("\n=== DONE ===")
