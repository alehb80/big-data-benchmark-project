DROP TABLE table_job3;
CREATE TABLE IF NOT EXISTS table_job3 AS 
SELECT t1.name, t1.sector, t2.data, t2.close, t2.ticker
FROM historical_stock AS t1 JOIN historical_stock_prices_string AS t2 on t2.ticker=t1.ticker
WHERE YEAR(data)>=2016 AND YEAR(data)<=2018 AND t1.sector!='N/A';


DROP VIEW name_data_min_max;
CREATE VIEW IF NOT EXISTS name_data_min_max AS 
SELECT name, ticker, sector, YEAR(data) AS anno, min(TO_DATE(data)) AS min_data, max(TO_DATE(data)) AS max_data 
FROM table_job3 
GROUP BY name, ticker, sector, YEAR(data);


DROP VIEW IF EXISTS name_data_close_min;
CREATE VIEW IF NOT EXISTS name_data_close_min AS 
SELECT b.name, a.ticker, YEAR(b.min_data) AS anno, SUM(a.close) AS min_close 
FROM table_job3 AS a, name_data_min_max AS b 
WHERE a.ticker=b.ticker AND a.data=b.min_data 
GROUP BY b.name,a.ticker, YEAR(b.min_data);


DROP VIEW IF EXISTS name_data_close_max;
CREATE VIEW IF NOT EXISTS name_data_close_max AS 
SELECT b.name, a.ticker, YEAR(b.max_data) AS anno, SUM(a.close) AS max_close 
FROM table_job3 AS a, name_data_min_max AS b 
WHERE a.data=b.max_data AND a.ticker=b.ticker
GROUP BY b.name, a.ticker, YEAR(b.max_data); 


DROP TABLE IF EXISTS name_anno_close;
CREATE TABLE IF NOT EXISTS name_anno_close AS 
SELECT ndc_mi.name, ndc_mi.ticker,ndc_mi.anno, ROUND((ndc_ma.max_close-ndc_mi.min_close)/ndc_mi.min_close *100, 0) AS perc_var_anno
FROM name_data_close_min AS ndc_mi, name_data_close_max AS ndc_ma
WHERE ndc_mi.name=ndc_ma.name AND ndc_mi.anno=ndc_ma.anno AND ndc_mi.ticker=ndc_ma.ticker
ORDER BY  ticker, anno;


DROP TABLE IF EXISTS name_anno_close_joined;
CREATE TABLE name_anno_close_joined AS 
SELECT name, concat_ws(',', collect_list(cast (perc_var_anno as STRING)))AS quotazione, anno
from name_anno_close
GROUP BY name, anno;


DROP TABLE IF EXISTS result_table_job3;
CREATE TABLE result_table_job3 AS
SELECT collect_set(v1.name), v1.quotazione as var16, v2.quotazione as var17, v3.quotazione as var18
FROM name_anno_close_joined v1, name_anno_close_joined v2, name_anno_close_joined v3
WHERE v1.name = v2.name AND v1.anno != v2.anno != v3.anno AND v1.name = v3.name AND v1.anno = '2016' and v2.anno = '2017' and v3.anno = '2018'
GROUP BY v1.quotazione, v2.quotazione, v3.quotazione
HAVING count(*)>1;   
