# Data Pipeline - DVC HCM Chatbot

Pipeline xử lý dữ liệu chat từ Chatbot Dịch vụ Công TP. Hồ Chí Minh.

## Kiến trúc

```
Oracle DB → Spark (K8s) → Iceberg (MinIO) → Trino → StarRocks (Gold) → Superset
```

## Cấu trúc thư mục

```
data/
├── docker/              # Dockerfiles + docker-compose
│   ├── Dockerfile       # Spark job image
│   ├── Dockerfile.hive  # Hive Metastore image
│   └── docker-compose.yml
├── kubernets/           # K8s manifests + Helm values
│   ├── hive-metastore.yaml
│   ├── postgres.yaml
│   ├── trino-values.yaml
│   ├── starrocks-values.yaml
│   └── superset-values.yaml
├── scripts/             # Python ETL scripts
│   ├── spark.py         # ETL: Excel → Iceberg
│   ├── dag.py           # Airflow DAG
│   └── etl_gold.py      # ETL: Iceberg → StarRocks Gold
├── sql/                 # SQL DDL + ETL
│   ├── starrocks_init.sql
│   ├── etl_gold.sql
│   └── gold_queries.sql
├── jars/                # PostgreSQL JDBC driver
└── core-site.xml        # Hadoop S3 config
```

## Local URLs & Credentials

| Dịch vụ | URL | User / Pass |
|---------|-----|-------------|
| **MinIO** (Data Lake) | http://localhost:9001 | `dtai16805` / `dtai16805` |
| **Superset** (BI Dashboard) | http://localhost:8088 | `admin` / `admin` |
| **Trino** (Query Engine) | http://localhost:8080 | — |
| **StarRocks** (OLAP) | MySQL port `localhost:9030` | `root` / *(trống)* |
| **Airflow** (Orchestration) | http://localhost:30080 | `admin` / `admin` |

> **Lưu ý**: Trino và StarRocks cần port-forward từ Minikube:
> ```bash
> kubectl port-forward -n trino svc/trino 8080:8080
> kubectl port-forward -n starrocks svc/my-starrocks-fe-service 9030:9030
> ```

## Hướng dẫn chạy

### Bước 1: Khởi động Docker services

```bash
docker compose up -d minio postgres-db superset
```

- **MinIO Console**: http://localhost:9001 (tạo bucket `iceberg-warehouse`)
- **Superset**: http://localhost:8088

### Bước 2: Khởi động Minikube

```bash
minikube start --memory=8g --cpus=4
```

### Bước 3: Deploy Trino + Hive Metastore

```bash
kubectl create namespace trino
kubectl apply -f kubernets/postgres.yaml
kubectl apply -f kubernets/hive-metastore.yaml
helm install trino trino/trino --namespace trino -f kubernets/trino-values.yaml
```

### Bước 4: Deploy StarRocks

```bash
kubectl create namespace starrocks
helm install starrocks starrocks/kube-starrocks --namespace starrocks -f kubernets/starrocks-values.yaml
```

### Bước 5: Chạy Spark ETL (tạo Iceberg table)

```bash
python scripts/spark.py
```

### Bước 6: Tạo Gold tables trong StarRocks

```bash
# Port-forward StarRocks
kubectl port-forward -n starrocks svc/my-starrocks-fe-service 9030:9030

# Chạy DDL (trong DBeaver hoặc MySQL client)
# Kết nối localhost:9030, chạy file sql/starrocks_init.sql
```

### Bước 7: Chạy Gold Layer ETL

```bash
python scripts/etl_gold.py
```

### Bước 8: Kết nối Superset → StarRocks

1. Mở http://localhost:8088
2. **Settings** → **Data** → **Databases** → **+ Database**
3. SQLAlchemy URI: `mysql+pymysql://root@host.docker.internal:9030/gold_db`
4. **Test Connection** → **Connect**

### Bước 9: Tạo Charts & Dashboard

```sql
-- Chart 1: Chat theo ngày (Line Chart)
SELECT log_date, total_messages FROM gold_db.gold_chat_stats_daily ORDER BY log_date;

-- Chart 2: Top Intent (Bar Chart)
SELECT intent_group, SUM(total_queries) AS total
FROM gold_db.gold_fallback_stats_daily GROUP BY intent_group ORDER BY total DESC;

-- Chart 3: Tỷ lệ fallback (Pie Chart)
SELECT intent_group, SUM(total_queries) AS total
FROM gold_db.gold_fallback_stats_daily GROUP BY intent_group;
```

## Tech Stack

| Thành phần | Công nghệ | Mục đích |
|-----------|-----------|----------|
| Data Lake | MinIO (S3) | Lưu trữ Iceberg tables |
| Processing | Spark 3.5.1 | ETL, xử lý dữ liệu |
| Catalog | Hive Metastore 4.0.0 | Đăng ký table metadata |
| Query Engine | Trino 435 | Query Iceberg từ StarRocks |
| OLAP | StarRocks 5.1.0 | Gold Layer, tổng hợp nhanh |
| BI | Apache Superset | Dashboard & Charts |
| Orchestration | Airflow | Schedule Spark jobs |
| Container | Docker | Local services |
| Orchestration | Kubernetes (Minikube) | Production-like K8s |
