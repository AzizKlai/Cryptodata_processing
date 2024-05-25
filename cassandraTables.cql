CREATE KEYSPACE IF NOT EXISTS crypto
  WITH REPLICATION = {
   'class' : 'SimpleStrategy', 'replication_factor' : 1
  };




CREATE TABLE IF NOT EXISTS crypto.crypto_realtime_prices(
    name_coin TEXT,
    symbol_coin TEXT,
    volume DECIMAL,
    market_cap DECIMAL,
    cir_supply DECIMAL,
    price DECIMAL,
    percent_change DOUBLE,
    percent_volume DOUBLE,
    percent_market_cap DOUBLE,
    time_stamp TIMESTAMP,
    PRIMARY KEY (symbol_coin, time_stamp)
) WITH CLUSTERING ORDER BY (time_stamp DESC);





CREATE TABLE IF NOT EXISTS crypto.crypto_rolling_means(
	symbol_coin VARCHAR,
	arithmetic_mean DECIMAL,
	geometric_mean DECIMAL,
	standard_deviation DECIMAL,
	start_time TIMESTAMP,
	end_time TIMESTAMP,
	PRIMARY KEY (symbol_coin, end_time)
) WITH CLUSTERING ORDER BY (end_time DESC);


CREATE TABLE IF NOT EXISTS crypto.market_share (
    time_stamp TIMESTAMP, 
    name_coin TEXT,
    percent_market_cap DOUBLE,
    percent_volume DOUBLE,
    PRIMARY KEY (time_stamp, percent_market_cap, name_coin)
) WITH CLUSTERING ORDER BY (percent_market_cap DESC);

CREATE TABLE IF NOT EXISTS crypto.biggest_winners (
    time_stamp TIMESTAMP,
    name_coin TEXT ,
    price DOUBLE,
    percent_change DOUBLE,
 PRIMARY KEY (time_stamp, percent_change, name_coin)
) WITH CLUSTERING ORDER BY (percent_change DESC);

CREATE TABLE IF NOT EXISTS crypto.biggest_losers (
    time_stamp TIMESTAMP,
    name_coin TEXT,
    price DOUBLE,
    percent_change DOUBLE,
 PRIMARY KEY (time_stamp, percent_change, name_coin)
) WITH CLUSTERING ORDER BY (percent_change DESC);






CREATE KEYSPACE IF NOT EXISTS crypto_batch
  WITH REPLICATION = {
   'class' : 'SimpleStrategy', 'replication_factor' : 1
  };


CREATE TABLE IF NOT EXISTS crypto_batch.crypto_month_agg (
    name TEXT,
    month TEXT,
    highest_high DECIMAL,
    lowest_low DECIMAL,
    first_open DECIMAL,
    last_close DECIMAL,
    total_volume DECIMAL,
    total_marketcap DECIMAL,
    PRIMARY KEY ((name, month))
);


CREATE TABLE IF NOT EXISTS crypto_batch.crypto_year_agg (
    name TEXT,
    year TEXT,
    highest_high DECIMAL,
    lowest_low DECIMAL,
    first_open DECIMAL,
    last_close DECIMAL,
    total_volume DECIMAL,
    total_marketcap DECIMAL,
    PRIMARY KEY ((name, year))
);

CREATE TABLE IF NOT EXISTS crypto_batch.crypto_summary (
    name TEXT,
    avg_price DECIMAL,
    min_price DECIMAL,
    max_price DECIMAL,
    max_market_cap DECIMAL,
    PRIMARY KEY (name)
);


CREATE TABLE IF NOT EXISTS crypto_batch.top_marketcap (
    name_coin TEXT,
    symbol_coin TEXT,
    date DATE,
    high DECIMAL,
    low DECIMAL,
    open DECIMAL,
    close DECIMAL,
    volume DECIMAL,
    market_cap DECIMAL,
    PRIMARY KEY (market_cap, name_coin)
);
