CREATE TABLE IF NOT EXISTS TransactionTime (
    id_time SERIAL PRIMARY KEY,
    t_date DATE NOT NULL,
    t_day INT NOT NULL,
    t_month VARCHAR(15) NOT NULL, --można zamienić na int
    t_year INT NOT NULL,
    quarter INT NOT NULL,
    day_of_week VARCHAR(15) NOT NULL --można zamienić na int
);

CREATE TABLE IF NOT EXISTS Market (
    id_market SERIAL PRIMARY KEY,
    market_name VARCHAR(15) NOT NULL
);
/*
--przykład
INSERT INTO Market(market_name) SELECT 'BTC-DOGE'
WHERE NOT EXISTS (
    SELECT 1 FROM Market WHERE market_name='BTC-DOGE'
);
INSERT INTO Market(market_name) SELECT 'USD-BTC'
WHERE NOT EXISTS (
    SELECT 1 FROM Market WHERE market_name='USD-BTC'
);
*/
CREATE TABLE IF NOT EXISTS TransactionType (
    id_type SERIAL PRIMARY KEY,
    t_type_name varchar(10) NOT NULL
);

/*
--przykład
INSERT INTO TransactionType(t_type_name) SELECT 'SELL'
WHERE NOT EXISTS (
    SELECT 1 FROM TransactionType WHERE t_type_name='SELL'
);

INSERT INTO TransactionType(t_type_name) SELECT 'BUY'
WHERE NOT EXISTS (
    SELECT 1 FROM TransactionType WHERE t_type_name='BUY'
);
*/

CREATE TABLE IF NOT EXISTS CryptoTransaction (
    id_time INT REFERENCES TransactionTime(id_time) NOT NULL,
    id_market INT REFERENCES Market(id_market) NOT NULL,
    id_type INT REFERENCES TransactionType(id_type) NOT NULL,
    PRIMARY KEY (id_time, id_market, id_type),
    quantity REAL NOT NULL,
    price REAL NOT NULL,
    total REAL NOT NULL
);

/*
możliwe, że można wykorzystać tabelę TransactionTime
do przechowywania tych czasów zamiast tworzyć nową
*/
CREATE TABLE IF NOT EXISTS EquityTime (
    id_time SERIAL PRIMARY KEY,
    t_date DATE NOT NULL,
    t_day VARCHAR(15) NOT NULL,
    t_month VARCHAR(15) NOT NULL,
    t_year INT NOT NULL,
    quarter INT NOT NULL,
    day_of_week INT NOT NULL
);

CREATE TABLE IF NOT EXISTS Company (
    id_company SERIAL PRIMARY KEY,
    company_name VARCHAR(40) NOT NULL
--    isin VARCHAR(40) NOT NULL --nie wiem czy potrzebne
);

/*
nazwy kolumn brałam stąd https://www.gpw.pl/price-archive?fetch=0&type=10&instrument=&date=28-05-2021&show_x=Show+results
nie ma tam ostatnich kolumn, które znajdują się w polskiej wersji
*/

CREATE TABLE IF NOT EXISTS Equity (
    id_time INT REFERENCES EquityTime(id_time) NOT NULL,
    id_company INT REFERENCES Company(id_company) NOT NULL,
    PRIMARY KEY (id_time, id_company),
    opening_price REAL NOT NULL,
    maximum_price REAL NOT NULL,
    minimum_price REAL NOT NULL,
    closing_price REAL NOT NULL,
    percent_price_change REAL NOT NULL,
    trade_volume REAL NOT NULL
--    transactions_number INT NOT NULL,
--    turnover_value REAL NOT NULL
);