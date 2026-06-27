FROM apache/spark:3.5.1

USER root

# Thêm --no-cache-dir để giảm dung lượng rác sau khi cài đặt
RUN pip install --no-cache-dir pandas openpyxl

ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.11.0/iceberg-spark-runtime-3.5_2.12-1.11.0.jar /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.11.0.jar
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/hadoop-aws-3.3.4.jar
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

# Set quyền cho thư viện
RUN chmod 644 /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.11.0.jar && \
    chmod 644 /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    chmod 644 /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

# Bỏ qua bước tải Maven vì JAR đã được nhúng sẵn
ENV SPARK_JARS_PACKAGES=

WORKDIR /opt/spark/work-dir/

# Nướng (Bake) code và data vào image
COPY spark.py .
COPY bot_dvc_hcm.xlsx .
COPY bot_ddvc_hcm_bot_predict.xlsx .

USER spark