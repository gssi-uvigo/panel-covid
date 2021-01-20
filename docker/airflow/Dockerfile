FROM apache/airflow:2.0.0-python3.8

USER root
RUN apt update
RUN apt install -y locales locales-all
ENV LC_ALL=en_US.UTF-8
ENV LC_CTYPE=en_US.UTF-8
ENV LANG=en_US.UTF-8

USER airflow
RUN pip install 'apache-airflow[mongo]'
RUN pip install --no-cache-dir --user beautifulsoup4 pandas PyPDF2