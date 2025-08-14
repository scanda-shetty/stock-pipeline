CREATE TABLE IF NOT EXISTS stock_prices (
    symbol TEXT NOT NULL,
    date VARCHAR(15) NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume VARCHAR,
    PRIMARY KEY (symbol, date)
);
