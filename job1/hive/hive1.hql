DROP VIEW parse_ticker_min_max_avg;
CREATE VIEW IF NOT EXISTS parse_ticker_min_max_avg AS
SELECT ticker, min(low) AS min_price, max(high) AS max_price, avg(volume) AS avg_volume
FROM historical_stock_prices_date
WHERE data>='2008-01-01' AND data<='2018-12-31' 
GROUP BY ticker;


DROP VIEW parse_2008;
CREATE VIEW IF NOT EXISTS parse_2008 AS
SELECT ticker, min(data) AS min_data
FROM historical_stock_prices_date
WHERE data>='2008-01-01' AND data<='2018-12-31'
GROUP BY ticker;


DROP VIEW parse2018;
CREATE VIEW IF NOT EXISTS parse2018 AS
SELECT ticker, max(data) AS max_data
FROM historical_stock_prices_date
WHERE data>='2008-01-01' AND data<='2018-12-31'
GROUP BY ticker;


DROP VIEW close_ticker_2008;
CREATE VIEW IF NOT EXISTS close_ticker_2008 AS
SELECT hspd.ticker, hspd.data, hspd.close
FROM parse_2008 AS min, historical_stock_prices_date AS hspd
WHERE hspd.ticker=min.ticker AND hspd.data=min.min_data;


DROP VIEW close_ticker_2018;
CREATE VIEW IF NOT EXISTS close_ticker_2018 AS
SELECT hspd.ticker, hspd.data, hspd.close
FROM parse2018 AS max, historical_stock_prices_date AS hspd
WHERE hspd.ticker=max.ticker AND hspd.data=max.max_data;


DROP VIEW changes_odds;
CREATE VIEW IF NOT EXISTS changes_odds AS
SELECT ci.ticker, FLOOR(((cf.close-ci.close)/ci.close)*100) AS var_perc
FROM close_ticker_2018 AS cf join close_ticker_2008  AS ci on cf.ticker=ci.ticker;


DROP TABLE result_table_job1;
CREATE TABLE IF NOT EXISTS result_table_job1 AS
SELECT a.ticker, b.var_perc, a.min_price, a.max_price, a.avg_volume
FROM parse_ticker_min_max_avg AS a join changes_odds AS b on a.ticker=b.ticker
ORDER BY b.var_perc DESC limit 10;
