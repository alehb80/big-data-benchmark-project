DROP TABLE main_table;
CREATE TABLE IF NOT EXISTS main_table AS
SELECT hs.sector, hsp.ticker, hsp.data, hsp.close, hsp.volume 
FROM historical_stock AS hs JOIN historical_stock_prices_string AS hsp ON hsp.ticker=hs.ticker 
WHERE YEAR(hsp.data)>=2008 AND YEAR(hsp.data)<=2018 AND hs.sector!='N/A';


DROP VIEW volume_sector;
CREATE VIEW IF NOT EXISTS volume_sector AS 
SELECT sector, YEAR(data) AS anno, SUM(volume) AS somma_volume, count(ticker) as conteggio
FROM main_table 
GROUP BY sector, YEAR(data);


DROP VIEW average_sector;
CREATE VIEW IF NOT EXISTS average_sector AS
SELECT sector, anno,ROUND((somma_volume/conteggio)) as avg_volume
FROM volume_sector;


DROP VIEW sector_data_min_max;
CREATE VIEW IF NOT EXISTS sector_data_min_max AS 
SELECT sector, ticker, min(TO_DATE(data)) AS min_data, max(TO_DATE(data)) AS max_data 
FROM main_table 
GROUP BY sector, ticker, YEAR(data);


DROP VIEW sector_data_close_2008;
CREATE VIEW IF NOT EXISTS sector_data_close_2008 AS 
SELECT b.sector, YEAR(b.min_data) AS anno, SUM(a.close) AS min_close 
FROM main_table AS a, sector_data_min_max AS b 
WHERE a.sector=b.sector AND a.data=b.min_data AND b.ticker=a.ticker 
GROUP BY b.sector, YEAR(b.min_data);


DROP VIEW sector_data_close_2018;
CREATE VIEW IF NOT EXISTS sector_data_close_2018 AS 
SELECT b.sector, YEAR(b.max_data) AS anno, SUM(a.close) AS max_close 
FROM main_table AS a, sector_data_min_max AS b 
WHERE a.sector=b.sector AND a.data=b.max_data AND b.ticker=a.ticker 
GROUP BY b.sector, YEAR(b.max_data);

DROP TABLE sector_data_close;
CREATE TABLE IF NOT EXISTS sector_data_close AS 
SELECT mi.sector, mi.anno, ROUND(AVG(((ma.max_close-mi.min_close)/mi.min_close)*100),2) AS perc_var_anno 
FROM sector_data_close_2008 AS mi, sector_data_close_2018 AS ma 
WHERE mi.sector=ma.sector AND mi.anno=ma.anno
GROUP BY mi.sector, mi.anno;


DROP VIEW sector_data_sum_close;
CREATE VIEW IF NOT EXISTS sector_data_sum_close AS 
SELECT sector, data, SUM(close) AS somma
FROM main_table 
GROUP BY sector, data;


DROP VIEW sector_data_avg_close;
CREATE VIEW IF NOT EXISTS sector_data_avg_close AS 
SELECT sector, YEAR(data) AS anno,AVG(somma) AS media 
FROM sector_data_sum_close 
GROUP BY sector, YEAR(data);


DROP TABLE result_table_job2;
CREATE TABLE IF NOT EXISTS result_table_job2 AS
SELECT a.sector, a.anno, c.avg_volume, b.perc_var_anno, a.media 
FROM sector_data_avg_close AS a, sector_data_close AS b, average_sector AS c 
WHERE a.sector=b.sector AND b.sector=c.sector AND a.anno=b.anno AND c.anno=b.anno
ORDER BY sector, anno;
