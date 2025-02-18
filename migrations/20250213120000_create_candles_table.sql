-- 20250213120000_create_candles_table.sql
CREATE TABLE candles (
    id SERIAL PRIMARY KEY,
    pair TEXT NOT NULL,
    time_frame TEXT NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    buy_base DOUBLE PRECISION NOT NULL,
    sell_base DOUBLE PRECISION NOT NULL,
    buy_quote DOUBLE PRECISION NOT NULL,
    sell_quote DOUBLE PRECISION NOT NULL,
    utc_begin BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS trades (
    tid TEXT PRIMARY KEY,
    pair TEXT,
    amount TEXT,
    side TEXT,
    quantity TEXT,
    create_time BIGINT,
    price TEXT,
    time_stamp BIGINT
);
