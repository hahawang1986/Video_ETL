# 使用官方 Apache Airflow 镜像作为基础镜像
FROM apache/airflow:2.9.1

# 切换到 root 用户以安装依赖项
USER root

# 安装 psycopg2 和其他依赖项
RUN apt-get update \
    && apt-get install -y libpq-dev gcc \
    && pip install psycopg2-binary

# 切换回 airflow 用户
USER airflow

