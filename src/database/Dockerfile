FROM postgres

ENV POSTGRES_PASSWORD=Admin123@ \
    POSTGRES_USER=admin \
    POSTGRES_DB=kafkadb

COPY . /docker-entrypoint-initdb.d/create_tables.sql