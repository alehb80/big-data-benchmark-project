CREATE TABLE IF NOT EXISTS historical_stock_prices_date (ticker STRING, open double,	close double,	adj_close double, low double, high double, volume int, data date)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/Users/alessio/Documents/Projects/bd_first_project/contents/partitioned-data/historical_stock_prices_half.csv'
OVERWRITE INTO TABLE historical_stock_prices_date;

CREATE TABLE IF NOT EXISTS historical_stock_prices_string (ticker STRING, open double,	close double,	adj_close double, low double, high double, volume int, data STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
TBLPROPERTIES("skip.header.line.count"="1");
LOAD DATA LOCAL INPATH '/Users/alessio/Documents/Projects/bd_first_project/contents/partitioned-data/historical_stock_prices_half.csv'
OVERWRITE INTO TABLE historical_stock_prices_string;

CREATE TABLE IF NOT EXISTS historical_stock (ticker STRING, exchanges STRING, name STRING, sector STRING, industry STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
       "separatorChar" = ",",
       "quoteChar"     = "\""
)
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/Users/alessio/Documents/Projects/bd_first_project/contents/historical_stocks.csv'
OVERWRITE INTO TABLE historical_stock;
