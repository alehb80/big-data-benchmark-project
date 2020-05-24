from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import SQLContext
import csv


def get_year(stringDate):
	return int(stringDate[:4])


def parse_l(l):
	csv_reader = csv.reader([l], delimiter=',')
	return next(csv_reader)


def close_price_max(x, y):
	if x[1] < y[1]:
		return y
	else:
		return x


def close_price_min(x, y):
	if x[1] > y[1]:
		return y
	else:
		return x


conf = SparkConf().setMaster("local[*]").setAppName("Job2")
sc = SparkContext(conf=conf)

hsp = sc.textFile("file:///Users/alessio/Documents/Projects/bd_first_project/contents/historical_stock_prices.csv") \
		.map(lambda l: parse_l(l)) \
		.filter(lambda l: l[0] != "ticker") \
		.filter(lambda l: 2008 <= get_year(l[7]) <= 2018)

hs = sc.textFile("file:///Users/alessio/Documents/Projects/bd_first_project/contents/historical_stocks.csv")\
	.map(lambda l: parse_l(l)) \
	.filter(lambda l: l[0] != "ticker") \
	.filter(lambda l: l[3] != "N/A")

join_hsp_hs = hsp\
	.map(lambda l: (l[0], (l[2], l[6], l[7])))\
	.join(hs.map(lambda l: (l[0], (l[3]))))

# returns an rdd with ticker, close, volume, specifics
join_hsp_hs = join_hsp_hs \
	.map(lambda l: (l[0], float(l[1][0][0]), float(l[1][0][1]), l[1][0][2], l[1][1]))

# persist the RDD
join_hsp_hs.persist(StorageLevel.MEMORY_AND_DISK)

# restituisce (settore,anno), volume annuale medio Medio ,
# quindi per ogni anno sommo tutti i volumi didelle azioni di quel settore e la divido per il numero di volumi
mean_volume = join_hsp_hs \
	.map(lambda l: ((l[4], l[3]), l[2])) \
	.map(lambda l: ((l[0][0], get_year(l[0][1])), (l[1], 1))) \
	.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])) \
	.map(lambda l: (l[0], (l[1][0]/l[1][1])))

# input (settore,specifics), close - faccio la somma --> mappo su (settore e anno),
# somma dei close --> faccio la media --> restituisce (settore,
# media dei close)
mean_sum_close = join_hsp_hs\
	.map(lambda l: ((l[4], l[3]), l[1])) \
	.reduceByKey(lambda x, y: x+y) \
	.map(lambda l: ((l[0][0], get_year(l[0][1])), (l[1], 1))) \
	.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])) \
	.map(lambda l: (l[0], (l[1][0]/l[1][1])))

# input (ticker, settore, anno), (close, specifics) - trovo minima specifics
# --> mappo su (settore, anno), close --> restitusco (settore, anno), somma dei close
min_close_total = join_hsp_hs \
	.map(lambda l: ((l[0], l[4], get_year(l[3])), (l[1], l[3]))) \
	.reduceByKey(lambda x, y: close_price_min(x, y)) \
	.map(lambda l: ((l[0][1], l[0][2]), l[1][0])) \
	.reduceByKey(lambda x, y: x+y)

max_close_total = join_hsp_hs \
	.map(lambda l: ((l[0], l[4], get_year(l[3])), (l[1], l[3]))) \
	.reduceByKey(lambda x, y: close_price_max(x, y)) \
	.map(lambda l: ((l[0][1], l[0][2]), l[1][0])) \
	.reduceByKey(lambda x, y: x+y)

join_price_variation = min_close_total.join(max_close_total)

price_variation = join_price_variation.map(lambda l: (l[0], round((l[1][1] - l[1][0]) / l[1][0] * 100, 2)))

stats = price_variation \
	.join(mean_sum_close) \
	.join(mean_volume) \
	.sortBy(lambda a: a[0]) \
	.map(lambda l: [l[0][0], l[0][1], l[1][0][0], l[1][0][1], l[1][1]])

stats.coalesce(1).saveAsTextFile("file:///Users/alessio/Documents/Projects/bd_first_project/results/job2/spark/")
