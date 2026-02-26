# Sử dụng Image chính chủ của Airflow
FROM apache/airflow:2.11.0-python3.10

USER root

# Cài đặt OpenJDK 17 
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless procps && \
    apt-get clean

# Thiết lập biến môi trường JAVA_HOME chuẩn của Java 17
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Copy file requirements và cài đặt thư viện Python
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt