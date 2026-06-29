# Data Pipeline - Chatbot

Pipeline xử lý dữ liệu chat từ Chatbot.

## Kiến trúc

```
Excel (Oracle DB) → Spark → Iceberg (MinIO) → Trino → StarRocks (Gold) → Superset
                              ↑                   ↑           ↑
                           Docker              Minikube     Minikube
```

## Phân bổ hạ tầng

| Thành phần | Nền tảng | 
|-----------|----------|
| MinIO | Docker | 
| PostgreSQL | Docker | 
| Superset | Docker | 
| StarRocks | Minikube | 
| Hive Metastore | Minikube | 
| Trino | Minikube | 
| Airflow | Minikube | 

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

### Docker Services

| Dịch vụ | URL | User / Pass |
|---------|-----|-------------|
| **MinIO** (Data Lake) | http://localhost:9001 | `dtai16805` / `dtai16805` |
| **Superset** (BI Dashboard) | http://localhost:8088 | `admin` / `admin` |

### Minikube Services (cần port-forward)

| Dịch vụ | Port-forward | URL | User / Pass |
|---------|-------------|-----|-------------|
| **StarRocks** (OLAP) | `kubectl port-forward -n starrocks svc/my-starrocks-fe-service 9030:9030` | MySQL `localhost:9030` | `root` / *(trống)* |
| **Trino** (Query) | `kubectl port-forward -n trino svc/trino 8081:8081` | http://localhost:8081 | — |
| **Airflow** (Orchestration) | `kubectl port-forward -n airflow svc/airflow-webserver 8080:8080` | http://localhost:8080 | `admin` / `admin` |

## Hướng dẫn chạy

### Bước 1: Khởi động Docker services

```bash
cd docker/
docker compose up -d minio postgres-db superset
```

### Bước 2: Khởi động Minikube

```bash
minikube start --memory=8g --cpus=4
```

### Bước 3: Deploy Hive Metastore + Trino

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

### Bước 5: Deploy Airflow

```bash
kubectl create namespace airflow
kubectl create configmap airflow-dags --from-file=scripts/dag.py -n airflow
helm install airflow apache-airflow/airflow --namespace airflow -f kubernets/airflow-values.yaml
```

### Bước 6: Chạy Spark ETL (tạo Iceberg table)

```bash
python scripts/spark.py
```

### Bước 7: Tạo Gold tables trong StarRocks

```bash
kubectl port-forward -n starrocks svc/my-starrocks-fe-service 9030:9030
# Chạy sql/starrocks_init.sql trong DBeaver (localhost:9030)
```

### Bước 8: Chạy Gold Layer ETL

```bash
python scripts/etl_gold.py
```

### Bước 9: Kết nối Superset → StarRocks

1. Mở http://localhost:8088
2. **Settings** → **Data** → **Databases** → **+ Database**
3. SQLAlchemy URI: `mysql+pymysql://root@host.docker.internal:9030/gold_db`
4. **Test Connection** → **Connect**

### Bước 10: Tạo Charts & Dashboard

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

| Thành phần | Công nghệ | Chạy trên | Mục đích |
|-----------|-----------|----------|----------|
| Data Lake | MinIO (S3) | Docker | Lưu trữ Iceberg tables |
| Processing | Spark 3.5.1 | Local/K8s | ETL, xử lý dữ liệu |
| Catalog | Hive Metastore 4.0.0 | Minikube | Đăng ký table metadata |
| Query Engine | Trino 435 | Minikube | Query Iceberg |
| OLAP | StarRocks 5.1.0 | Minikube | Gold Layer, tổng hợp nhanh |
| BI | Apache Superset | Docker | Dashboard & Charts |
| Orchestration | Airflow 2.10.5 | Minikube | Schedule Spark jobs |
| Container | Docker Desktop | — | Local services |
| Cluster | Kubernetes (Minikube) | — | Production-like K8s |
