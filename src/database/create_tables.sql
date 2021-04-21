CREATE TABLE IF NOT EXISTS messages(
    id SERIAL,
    number BIGINT
);
CREATE TABLE IF NOT EXISTS consumers_status(
    id SERIAL,
    status VARCHAR
);