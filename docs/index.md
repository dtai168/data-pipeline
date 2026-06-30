# 📚 Data Pipeline - Chatbot Documentation

> **Phiên bản:** 1.0  
> **Cập nhật:** 2026-06-29

---

## 📖 Tài liệu

| Tài liệu | Mô tả | Liên kết |
|---------|-------|----------|
| **Documentation** | Hướng dẫn chi tiết từng bước với kết quả mong đợi | [DOCUMENTATION.md](DOCUMENTATION.md) |

---

## 🚀 Bắt đầu nhanh

### Yêu cầu

- Docker Desktop
- Minikube
- kubectl
- Helm
- Python 3.8+
- DBeaver (optional)

### Cài đặt

```bash
# 1. Clone repository
git clone <repository-url>
cd data

# 2. Khởi động Docker services
cd docker/
docker compose up -d minio postgres-db superset

# 3. Khởi động Minikube
minikube start --memory=8g --cpus=4

# 4. Deploy services trên K8s
kubectl create namespace trino
kubectl apply -f kubernets/postgres.yaml
kubectl apply -f kubernets/hive-metastore.yaml
helm install trino trino/trino --namespace trino -f kubernets/trino-values.yaml

kubectl create namespace starrocks
helm install starrocks starrocks/kube-starrocks --namespace starrocks -f kubernets/starrocks-values.yaml

# 5. Chạy ETL
python scripts/spark.py
python scripts/etl_gold.py
```

### URLs

| Dịch vụ | URL |
|---------|-----|
| MinIO Console | http://localhost:9001 |
| Superset | http://localhost:8088 |
| Trino | http://localhost:8081 |
| Airflow | http://localhost:8080 |

---

## 📊 Kiến trúc tổng quát

```
Excel (Oracle DB) → Spark → Iceberg (MinIO) → Trino → StarRocks (Gold) → Superset
                              ↑                   ↑           ↑
                           Docker              Minikube     Minikube
```

---

## 📧 Liên hệ

- **Tác giả:** Duong Van Tai
- **Email:** tai.dv@hus.edu.vn
