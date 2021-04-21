FROM postgres

ENV POSTGRES_PASSWORD=Admin123@ \
    POSTGRES_USER=admin \
    POSTGRES_DB=kafkadb

COPY create_tables.sql /docker-entrypoint-initdb.d/create_tables.sql