FROM apache/airflow:2.2.4

COPY requirements.txt .

RUN pip install -r requirements.txt

# USER root
# RUN locale-gen en_US.UTF-8
# ENV LANG en_US.UTF-8
# ENV LANGUAGE en_US:en
# ENV LC_ALL en_US.UTF-8

# USER airflow