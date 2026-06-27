import os
import re
import json
import shutil
import tempfile
import pandas as pd
from pyspark.sql import SparkSession

# -- Environment variables for S3/MinIO configuration ---------------------------
S3_ENDPOINT        = os.environ.get("S3_ENDPOINT", "http://127.0.0.1:9000")
S3_ACCESS_KEY      = os.environ.get("S3_ACCESS_KEY", "dtai16805")
S3_SECRET_KEY      = os.environ.get("S3_SECRET_KEY", "dtai16805")
HIVE_METASTORE_URI = os.environ.get("HIVE_METASTORE_URI", "")
ICEBERG_CATALOG_TYPE = os.environ.get("ICEBERG_CATALOG_TYPE", "hadoop")
# Auto-switch to hive catalog when Hive Metastore URI is provided
if not os.environ.get("ICEBERG_CATALOG_TYPE") and HIVE_METASTORE_URI:
    ICEBERG_CATALOG_TYPE = "hive"

if os.name == "nt":
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

# -- Constants ------------------------------------------------------------------
CHECKPOINT_FILE   = "checkpoint_last_processed.json"
CHAT_DATA_PATH    = "bot_dvc_hcm.xlsx"
PREDICT_DATA_PATH = "bot_ddvc_hcm_bot_predict.xlsx"
OUTPUT_TABLE      = "iceberg.chatbot_db.processed_logs"
TEMP_DIR          = os.path.join(tempfile.gettempdir(), "chatbot_etl")

_ORACLE_TS_RE = re.compile(r'(\d{2})\.(\d{2})\.(\d{2})(\.\d+)?')


# -- Spark Session ---------------------------------------------------------------
def get_spark():
    builder = (
        SparkSession.builder
        .appName("ChatbotETL_AutoUpdate")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", ICEBERG_CATALOG_TYPE)
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://iceberg-warehouse/chatbot_data")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
    )
    if HIVE_METASTORE_URI:
        builder = builder.config("spark.sql.catalog.iceberg.uri", HIVE_METASTORE_URI)

    # SPARK_JARS_PACKAGES: "" = Docker (skip), unset = local dev (defaults), else custom
    packages_env = os.environ.get("SPARK_JARS_PACKAGES")
    if packages_env is not None:
        if packages_env:
            builder = builder.config("spark.jars.packages", packages_env)
    else:
        builder = builder.config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )

    return builder.getOrCreate()


# -- Helpers ---------------------------------------------------------------------
def save_checkpoint(ts):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"last_processed_timestamp": ts}, f)


def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return None
    with open(CHECKPOINT_FILE, "r") as f:
        return json.load(f).get("last_processed_timestamp")


def table_exists(spark):
    try:
        spark.sql(f"DESCRIBE TABLE {OUTPUT_TABLE}")
        return True
    except Exception:
        return False


def _parse_oracle_ts(series):
    """Parse Oracle datetime strings (dots -> colons) into pandas Timestamps."""
    if not pd.api.types.is_string_dtype(series):
        return series
    normalized = series.apply(
        lambda x: _ORACLE_TS_RE.sub(r'\1:\2:\3\4', str(x)) if pd.notna(x) else x
    )
    return pd.to_datetime(normalized, dayfirst=True, errors="coerce")


# -- ETL in pandas ---------------------------------------------------------------
def process_in_pandas(last_ts):
    print("--> Reading Excel files with pandas...")
    chat = pd.read_excel(CHAT_DATA_PATH, engine="openpyxl")
    predict = pd.read_excel(PREDICT_DATA_PATH, engine="openpyxl")
    print(f"    chat rows: {len(chat)}, predict rows: {len(predict)}")

    # Parse Oracle-style datetime BEFORE filtering
    for col in ["CREATED_TIME", "LAST_UPDATED_TIME"]:
        if col in chat.columns:
            chat[col] = _parse_oracle_ts(chat[col])
        if col in predict.columns:
            predict[col] = _parse_oracle_ts(predict[col])

    # Incremental filter
    if last_ts:
        cutoff = pd.to_datetime(last_ts)
        chat = chat[chat["CREATED_TIME"] > cutoff]
        predict = predict[
            (predict["CREATED_TIME"] > cutoff)
            | (predict["LAST_UPDATED_TIME"] > cutoff)
        ]
        print(f"    after filter -> chat: {len(chat)}, predict: {len(predict)}")

    chat = chat.drop_duplicates(subset=["ID"])
    predict = predict.drop_duplicates(subset=["ID"])

    print("--> Joining and applying intent logic...")
    m = predict.merge(chat, left_on="ID_CHATLOG", right_on="ID",
                      how="left", suffixes=("", "_chat"))
    m = m.drop(columns=[c for c in m.columns if c.endswith("_chat")], errors="ignore")

    m["FINAL_INTENT"] = m.apply(
        lambda r: "fallback" if r["INTENT_NAME"] == "fallback"
        else "bot_khong_hieu" if (pd.notna(r.get("INTENT_CONFIDENCE"))
                                  and pd.notna(r.get("NLU_THRESHOLD"))
                                  and r["INTENT_CONFIDENCE"] < r["NLU_THRESHOLD"])
        else r["INTENT_NAME"], axis=1)
    print(f"    merged rows: {len(m)}")
    return m


# -- Write via Parquet bypass (avoids PythonRunner on Windows) -------------------
def write_via_parquet(spark, pdf):
    os.makedirs(TEMP_DIR, exist_ok=True)
    pq_path = os.path.join(TEMP_DIR, "batch.parquet")

    # Uppercase columns for Iceberg consistency, datetime -> string for Parquet
    pdf.columns = [c.upper() for c in pdf.columns]
    for col in pdf.columns:
        if pd.api.types.is_datetime64_any_dtype(pdf[col]):
            pdf[col] = pdf[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    print(f"    Writing Parquet to {pq_path}...")
    pdf.to_parquet(pq_path, engine="pyarrow", index=False)

    view_name = "_batch_staging"
    spark.read.parquet(pq_path).createOrReplaceTempView(view_name)

    if not table_exists(spark):
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.chatbot_db")
        print(f"    Creating table {OUTPUT_TABLE}...")
        spark.sql(f"CREATE TABLE {OUTPUT_TABLE} USING iceberg AS SELECT * FROM {view_name}")
    else:
        existing_cols = [r.col_name for r in spark.sql(
            f"DESCRIBE TABLE {OUTPUT_TABLE}"
        ).collect() if not r.col_name.startswith('#') and r.col_name != '']

        if set(existing_cols) != set(pdf.columns):
            print(f"    Schema mismatch ({len(existing_cols)} vs {len(pdf.columns)} cols). Recreating...")
            spark.sql(f"DROP TABLE IF EXISTS {OUTPUT_TABLE}")
            spark.sql(f"CREATE TABLE {OUTPUT_TABLE} USING iceberg AS SELECT * FROM {view_name}")
        else:
            print(f"    Appending to {OUTPUT_TABLE}...")
            spark.sql(f"INSERT INTO {OUTPUT_TABLE} SELECT * FROM {view_name}")

    spark.catalog.dropTempView(view_name)
    shutil.rmtree(TEMP_DIR, ignore_errors=True)


# -- Main ETL -------------------------------------------------------------------
def run_etl():
    spark = get_spark()
    last_ts = load_checkpoint()
    print(f"--> Last checkpoint: {last_ts}")

    result = process_in_pandas(last_ts)
    if result.empty:
        print("--> No new data to process.")
        return

    # Dedup against existing table
    try:
        if table_exists(spark):
            existing = spark.sql(f"SELECT ID FROM {OUTPUT_TABLE}").toPandas()
            before = len(result)
            result = result[~result["ID"].isin(existing["ID"])]
            print(f"    removed {before - len(result)} duplicate rows")
    except Exception as e:
        print(f"    dedup skipped: {e}")

    if result.empty:
        print("--> No new data (all duplicates).")
        return

    print(f"--> Writing {len(result)} records to {OUTPUT_TABLE}...")
    write_via_parquet(spark, result)

    # Update checkpoint
    max_ts = result["CREATED_TIME"].max()
    if pd.notna(max_ts):
        save_checkpoint(pd.Timestamp(max_ts).isoformat())
        print(f"--> Checkpoint saved at {max_ts}")
    print("--> Done.")
    spark.stop()


if __name__ == "__main__":
    run_etl()
