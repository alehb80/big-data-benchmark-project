from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import SQLContext
import csv

conf = SparkConf().setMaster("local[*]").setAppName("Job3")
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
	  	.filter(lambda linea: get_anno(linea[7]) >= 2016 and get_anno(linea[7]) <= 2018)

hs = sc.textFile("file:///home/hadoop/datasets/historical_stocks.csv") \
 	   .map(lambda linea: parse_linea(linea)) \
 	   .filter(lambda linea: linea[0] != "ticker") \
	   .filter(lambda linea: linea[3] != "N/A")

join_hsp_hs = hsp \
			  .map(lambda linea: (linea[0], (linea[2], linea[7]))) \
			  .join(hs.map(lambda linea: (linea[0], linea[2])))

# restituisce un rdd con ticker, close, data, name,
join_hsp_hs = join_hsp_hs.map(lambda linea: (linea[0], float(linea[1][0][0]),
											linea[1][0][1], linea[1][1]))

# persist the RDD
join_hsp_hs.persist(StorageLevel.MEMORY_AND_DISK)

# input (ticker, settore, anno, name), (close, data) - trovo minima data -->
# mappo su (settore, anno), close --> restitusco (settore, anno),
# somma dei close
data_close_minimo = join_hsp_hs \
				 .map(lambda linea: ((linea[0], get_anno(linea[2]), linea[3]), (linea[1],linea[2]))) \
				 .reduceByKey(lambda x, y: close_minimo(x, y)) \
				 .map(lambda linea: ((linea[0][1], linea[0][2]), linea[1][0])) \
				 .reduceByKey(lambda x, y: x+y)

data_close_massimo = join_hsp_hs \
				 .map(lambda linea: ((linea[0], get_anno(linea[2]), linea[3]), (linea[1],linea[2]))) \
				 .reduceByKey(lambda x, y: close_massimo(x, y)) \
				 .map(lambda linea: ((linea[0][1], linea[0][2]), linea[1][0])) \
				 .reduceByKey(lambda x, y: x+y)

join_incremento_percentuale = data_close_minimo \
				.join(data_close_massimo)

incremento_percentuale = join_incremento_percentuale \
		   .map(lambda linea: (linea[0],
		   					  round((linea[1][1]-linea[1][0])/linea[1][0] * 100, 0)))

tripletta_trend = incremento_percentuale \
			.map(lambda linea: (linea[0][1], ([(linea[0][0], linea[1])]))) \
			.reduceByKey(lambda x, y: x+y)

tripletta_trend_pulito = tripletta_trend.filter(lambda linea: len(linea[1]) == 3)



def ordinamento_tripletta(x):
	dict = []
	if x[1][0][0] < x[1][1][0] and x[1][0][0] < x[1][2][0]:
		dict.append(x[1][0])
		if x[1][1][0] < x[1][2][0]:
			dict.append(x[1][1])
			dict.append(x[1][2])
		else:
			dict.append(x[1][2])
			dict.append(x[1][1])
	else:
		if x[1][1][0] < x[1][0][0] and x[1][1][0] < x[1][2][0]:
			dict.append(x[1][1])
			if x[1][0][0] < x[1][2][0]:
				dict.append(x[1][0])
				dict.append(x[1][2])
			else:
				dict.append(x[1][2])
				dict.append(x[1][0])
		else:
			dict.append(x[1][2])
			if x[1][0][0] < x[1][1][0]:
				dict.append(x[1][0])
				dict.append(x[1][1])
			else:
				dict.append(x[1][1])
				dict.append(x[1][0])
	return (x[0], dict)


tripletta_trend_pulito_order = tripletta_trend_pulito \
						.map(lambda linea: ordinamento_tripletta(linea))
output = tripletta_trend_pulito_order \
		 .map(lambda linea: (tuple(linea[1]),([(linea[0])]))) \
		 .reduceByKey(lambda x,y: x+y).filter(lambda linea: len(linea[1])>=2) \
		 .map(lambda linea: (linea[1],linea[0][0],linea[0][1],linea[0][2])) \
		 .sortBy(lambda a: a[1])



output.coalesce(1).saveAsTextFile("file:///home/hadoop/output/output_job3/")
