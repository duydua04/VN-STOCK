FROM apache/airflow:2.11.0-python3.10

USER root

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk-headless && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt