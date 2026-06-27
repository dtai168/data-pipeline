-- ============================================================
-- StarRocks Gold Layer - DDL
-- Kết nối DBeaver vào localhost:9030 (root, no password)
-- Chạy TỪNG BƯỚC riêng, mỗi bước 1 lần
-- ============================================================

-- BƯỚC 1: Tạo database (chạy 1 lần)
CREATE DATABASE IF NOT EXISTS gold_db;

-- BƯỚC 2: Tạo Gold Tables (chạy sau bước 1, mỗi CREATE chạy riêng)

-- Bảng 1: Thống kê chat & user theo ngày (BITMAP)
CREATE TABLE gold_db.gold_chat_stats_daily (
    log_date        DATE            COMMENT 'Ngày thống kê',
    total_messages  BIGINT SUM DEFAULT "0" COMMENT 'Tổng số tin nhắn',
    unique_users    BITMAP BITMAP_UNION COMMENT 'User duy nhất (bitmap)'
)
AGGREGATE KEY(log_date)
DISTRIBUTED BY HASH(log_date) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- Bảng 2: Tỷ lệ fallback theo intent
CREATE TABLE gold_db.gold_fallback_stats_daily (
    log_date        DATE            COMMENT 'Ngày thống kê',
    intent_group    VARCHAR(100)    COMMENT 'Nhóm ý định',
    total_queries   BIGINT SUM DEFAULT "0" COMMENT 'Tổng câu hỏi',
    fallback_queries BIGINT SUM DEFAULT "0" COMMENT 'Số fallback'
)
AGGREGATE KEY(log_date, intent_group)
DISTRIBUTED BY HASH(intent_group) BUCKETS 3
PROPERTIES ("replication_num" = "1");
