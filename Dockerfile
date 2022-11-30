FROM --platform=linux/amd64  apache/airflow:2.3.0
RUN pip install 'apache-airflow[amazon]'