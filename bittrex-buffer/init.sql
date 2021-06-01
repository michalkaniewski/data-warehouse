/*
CREATE TABLE IF NOT EXISTS numbers (
    number BIGINT,
    timestamp BIGINT
);
*/

CREATE TABLE IF NOT EXISTS BittrexBuffer (
    id_buffer SERIAL PRIMARY KEY,
    id_transaction BIGINT NOT NULL,
    b_date DATE NOT NULL,
    b_day INT NOT NULL,
    b_month VARCHAR(15) NOT NULL, --można zamienić na int
    b_year INT NOT NULL,
    quarter INT NOT NULL,
    day_of_week VARCHAR(15) NOT NULL, --można zamienić na int
    market varchar(15) not null,
    quantity REAL NOT NULL,
    price REAL NOT NULL,
    total REAL NOT NULL,
    order_type CHAR(4) NOT NULL
);