from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import SQLContext
import csv

conf = SparkConf().setMaster("local[*]").setAppName("Job2")
sc = SparkContext(conf=conf)

def get_anno(dateAsString):
	return int(dateAsString[:4])

def parse_linea(linea):
	csv_reader = csv.reader([linea], delimiter=',')
	return next(csv_reader)

def close_minimo(x, y):
	if x[1] > y[1]:
		return y
	else:
		return x

def close_massimo(x, y):
	if x[1] < y[1]:
		return y
	else:
		return x

hsp = sc.textFile("file:///home/hadoop/datasets/historical_stock_prices.csv") \
		.map(lambda linea: parse_linea(linea)) \
		.filter(lambda linea: linea[0] != "ticker") \
		.filter(lambda linea: get_anno(linea[7]) >= 2008 and get_anno(linea[7]) <= 2018)

hs = sc.textFile("file:///home/hadoop/datasets/historical_stocks.csv") \
 	   .map(lambda linea: parse_linea(linea)) \
 	   .filter(lambda linea: linea[0] != "ticker") \
	   .filter(lambda linea: linea[3] != "N/A")

join_hsp_hs = hsp \
			  .map(lambda linea: (linea[0], (linea[2], linea[6], linea[7]))) \
			  .join(hs.map(lambda linea: (linea[0], (linea[3]))))

# restituisce un rdd con ticker, close, volume, data, settore
join_hsp_hs = join_hsp_hs \
			  .map(lambda linea: (linea[0], float(linea[1][0][0]), float(linea[1][0][1]), linea[1][0][2],
			  					linea[1][1]))

# persist the RDD
join_hsp_hs.persist(StorageLevel.MEMORY_AND_DISK)

# restituisce (settore,anno), volume annuale medio Medio , quindi per ogni anno sommo tutti i volumi didelle azioni di quel settore e la divido per il numero di volumi
volume_medio_annuale = join_hsp_hs \
			 .map(lambda linea: ((linea[4], linea[3]), linea[2])) \
			 .map(lambda linea: ((linea[0][0], get_anno(linea[0][1])), (linea[1], 1))) \
			 .reduceByKey(lambda x ,y:(x[0]+y[0],x[1]+y[1]))\
			 .map(lambda linea: (linea[0], (linea[1][0]/linea[1][1])))
# input (settore,data), close - faccio la somma --> mappo su (settore e anno),
# somma dei close --> faccio la media --> restituisce (settore,
# media dei close)
somma_medie_close = join_hsp_hs \
				.map(lambda linea: ((linea[4], linea[3]), linea[1])) \
				.reduceByKey(lambda x, y: x+y) \
				.map(lambda linea: ((linea[0][0], get_anno(linea[0][1])), (linea[1], 1))) \
				.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])) \
				.map(lambda linea: (linea[0], (linea[1][0]/linea[1][1])))

# input (ticker, settore, anno), (close, data) - trovo minima data --> mappo su (settore, anno), close --> restitusco (settore, anno), somma dei close
data_close_minimo = join_hsp_hs \
				 .map(lambda linea: ((linea[0], linea[4], get_anno(linea[3])), (linea[1], linea[3]))) \
				 .reduceByKey(lambda x, y: close_minimo(x, y)) \
				 .map(lambda linea: ((linea[0][1], linea[0][2]), linea[1][0])) \
				 .reduceByKey(lambda x, y: x+y)

data_close_massimo = join_hsp_hs \
				 .map(lambda linea: ((linea[0], linea[4], get_anno(linea[3])), (linea[1], linea[3]))) \
				 .reduceByKey(lambda x, y: close_massimo(x, y)) \
				 .map(lambda linea: ((linea[0][1], linea[0][2]), linea[1][0])) \
				 .reduceByKey(lambda x, y: x+y)

join_incremento_percentuale = data_close_minimo.join(data_close_massimo)

incremento_percentuale = join_incremento_percentuale.map(lambda linea: (linea[0], round((linea[1][1]-linea[1][0])/linea[1][0] * 100, 2) ))

output = incremento_percentuale \
		 .join(somma_medie_close) \
		 .join(volume_medio_annuale) \
		 .sortBy(lambda a: a[0]) \
		 .map(lambda linea: [linea[0][0], linea[0][1], linea[1][0][0], linea[1][0][1],
		                    linea[1][1]])

output.coalesce(1).saveAsTextFile("file:///home/hadoop/output/output_job2/")
