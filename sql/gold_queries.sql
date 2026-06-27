-- ============================================================
-- StarRocks Gold - Queries kiểm tra & quản lý
-- Chạy trong DBeaver kết nối localhost:9030
-- ============================================================

-- Kiểm tra Gold tables
SELECT * FROM gold_db.gold_chat_stats_daily ORDER BY log_date;
SELECT * FROM gold_db.gold_fallback_stats_daily ORDER BY log_date, intent_group;

-- Chạy Task ngay lập tức (test)
SUBMIT TASK task_etl_daily_chat_stats;
SUBMIT TASK task_etl_fallback_stats;

-- Xem danh sách Tasks
SHOW TASKS;

-- Xem lịch sử chạy Tasks
SELECT * FROM information_schema.task_runs
ORDER BY create_time DESC LIMIT 10;
