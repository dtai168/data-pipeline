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

## Triển khai

### 1. MinIO + Hive Metastore
```bash
docker compose up -d minio minio-init postgres-hive hive-metastore
```

### 2. Spark ETL (Local)
```bash
python scripts/spark.py
```

### 3. StarRocks (Minikube)
```bash
helm install starrocks starrocks/kube-starrocks --namespace starrocks -f kubernets/starrocks-values.yaml
```

### 4. Trino (Minikube)
```bash
helm install trino trino/trino --namespace trino -f kubernets/trino-values.yaml
```

### 5. Gold Layer ETL
```bash
python scripts/etl_gold.py
```

### 6. Superset (Docker)
```bash
docker compose up -d superset
# http://localhost:8088 (admin/admin)
```

## Tech Stack

| Thành phần | Công nghệ |
|-----------|-----------|
| Data Lake | MinIO (S3) |
| Processing | Spark 3.5.1 |
| Catalog | Hive Metastore 4.0.0 |
| Query Engine | Trino 435 |
| OLAP | StarRocks 5.1.0 |
| BI | Apache Superset |
| Orchestration | Airflow |
| Orchestration | Kubernetes (Minikube) |
