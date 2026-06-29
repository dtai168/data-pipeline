from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

default_args = {
    'owner': 'dtai',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'lakehouse_chatbot_etl',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['spark', 'iceberg'],
    ) as dag:

    run_spark_job = KubernetesPodOperator(
        namespace='airflow',
        image='chatbot-spark-job:v4',
        name='spark-etl-task',
        task_id='run_pyspark_iceberg_ingestion',
        image_pull_policy='Never',
        get_logs=True,
        is_delete_operator_pod=True,
        env_vars={
            'HIVE_METASTORE_URI': '',
            'ICEBERG_WAREHOUSE': 's3a://iceberg-warehouse/chatbot_data',
            'S3_ENDPOINT': 'http://host.minikube.internal:9000',
            'S3_ACCESS_KEY': 'dtai16805',
            'S3_SECRET_KEY': 'dtai16805',
            'ICEBERG_CATALOG_TYPE': 'hadoop',
            'SPARK_JARS_PACKAGES': '',
        },
    )

    run_spark_job
