FROM postgres:9.6-alpine
COPY redash-covid-* /docker-entrypoint-initdb.d/
RUN cd /docker-entrypoint-initdb.d && cat redash-covid-?? > redash-covid.sql && rm redash-covid-*