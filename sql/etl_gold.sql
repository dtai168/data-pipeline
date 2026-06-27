-- ============================================================
-- StarRocks Gold Layer - ETL Tasks
-- Chạy trong DBeaver kết nối localhost:9030
--
-- created_time la varchar "2024-07-02 15:40:45"
-- Dung date_parse de convert sang DATE
-- ============================================================

-- Bat config Task
ADMIN SET FRONTEND CONFIG ("empty_load_as_error" = "false");

-- Task 1: Tong hop chat + user theo ngay (01:00 AM)
CREATE TASK IF NOT EXISTS gold_db.task_etl_daily_chat_stats
SCHEDULE EVERY 1 DAY STARTS "2026-06-27 01:00:00"
AS
INSERT INTO gold_db.gold_chat_stats_daily
SELECT
    CAST(date_parse(created_time, '%Y-%m-%d %H:%i:%s') AS DATE) AS log_date,
    COUNT(1) AS total_messages,
    bitmap_hash(sender_id) AS unique_users
FROM iceberg_catalog.chatbot_db.processed_logs
WHERE created_time IS NOT NULL
  AND created_time != ''
  AND CAST(date_parse(created_time, '%Y-%m-%d %H:%i:%s') AS DATE) = CURRENT_DATE() - INTERVAL 1 DAY
GROUP BY CAST(date_parse(created_time, '%Y-%m-%d %H:%i:%s') AS DATE);

-- Task 2: Ti le fallback theo intent (01:05 AM)
CREATE TASK IF NOT EXISTS gold_db.task_etl_fallback_stats
SCHEDULE EVERY 1 DAY STARTS "2026-06-27 01:05:00"
AS
INSERT INTO gold_db.gold_fallback_stats_daily
SELECT
    CAST(date_parse(created_time, '%Y-%m-%d %H:%i:%s') AS DATE) AS log_date,
    final_intent AS intent_group,
    COUNT(1) AS total_queries,
    SUM(CASE WHEN final_intent = 'fallback' THEN 1 ELSE 0 END) AS fallback_queries
FROM iceberg_catalog.chatbot_db.processed_logs
WHERE created_time IS NOT NULL
  AND created_time != ''
  AND CAST(date_parse(created_time, '%Y-%m-%d %H:%i:%s') AS DATE) = CURRENT_DATE() - INTERVAL 1 DAY
GROUP BY CAST(date_parse(created_time, '%Y-%m-%d %H:%i:%s') AS DATE), final_intent;
