from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import SQLContext
import csv
import json


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


def order_trend(x):
	trend = []
	if x[1][0][0] < x[1][1][0] and x[1][0][0] < x[1][2][0]:
		trend.append(x[1][0])
		if x[1][1][0] < x[1][2][0]:
			trend.append(x[1][1])
			trend.append(x[1][2])
		else:
			trend.append(x[1][2])
			trend.append(x[1][1])
	else:
		if x[1][1][0] < x[1][0][0] and x[1][1][0] < x[1][2][0]:
			trend.append(x[1][1])
			if x[1][0][0] < x[1][2][0]:
				trend.append(x[1][0])
				trend.append(x[1][2])
			else:
				trend.append(x[1][2])
				trend.append(x[1][0])
		else:
			trend.append(x[1][2])
			if x[1][0][0] < x[1][1][0]:
				trend.append(x[1][0])
				trend.append(x[1][1])
			else:
				trend.append(x[1][1])
				trend.append(x[1][0])
	return x[0], trend


conf = SparkConf().setMaster("local[*]").setAppName("Job3")
sc = SparkContext(conf=conf)

hsp = sc.textFile("file:///Users/alessio/Documents/Projects/bd_first_project/contents/historical_stock_prices.csv") \
	.map(lambda l: parse_l(l)) \
	.filter(lambda l: l[0] != "ticker") \
	.filter(lambda l: 2016 <= get_year(l[7]) <= 2018)

hs = sc.textFile("file:///Users/alessio/Documents/Projects/bd_first_project/contents/historical_stocks.csv")\
	.map(lambda l: parse_l(l))\
	.filter(lambda l: l[0] != "ticker") \
	.filter(lambda l: l[3] != "N/A")

join_hsp_hs = hsp \
	.map(lambda l: (l[0], (l[2], l[7]))) \
	.join(hs.map(lambda l: (l[0], l[2])))

# restituisce un rdd con ticker, close, specifics, name,
join_hsp_hs = join_hsp_hs.map(lambda l: (l[0], float(l[1][0][0]), l[1][0][1], l[1][1]))

# persist the RDD
join_hsp_hs.persist(StorageLevel.MEMORY_AND_DISK)

# input (ticker, settore, anno, name), (close, specifics) - trovo minima specifics -->
# mappo su (settore, anno), close --> restitusco (settore, anno),
# somma dei close
min_close_total = join_hsp_hs \
	.map(lambda l: ((l[0], get_year(l[2]), l[3]), (l[1], l[2]))) \
	.reduceByKey(lambda x, y: close_price_min(x, y)) \
	.map(lambda l: ((l[0][1], l[0][2]), l[1][0])) \
	.reduceByKey(lambda x, y: x+y)

max_close_total = join_hsp_hs \
	.map(lambda l: ((l[0], get_year(l[2]), l[3]), (l[1], l[2]))) \
	.reduceByKey(lambda x, y: close_price_max(x, y)) \
	.map(lambda l: ((l[0][1], l[0][2]), l[1][0])) \
	.reduceByKey(lambda x, y: x+y)

join_price_variation = min_close_total \
	.join(max_close_total)

price_variation = join_price_variation \
	.map(lambda l: (l[0], round((l[1][1]-l[1][0])/l[1][0] * 100, 0)))

trend = price_variation \
	.map(lambda l: (l[0][1], ([(l[0][0], l[1])]))) \
	.reduceByKey(lambda x, y: x+y)

trend_clean = trend.filter(lambda l: len(l[1]) == 3)

trend_clean_order = trend_clean \
	.map(lambda l: order_trend(l))

stats = trend_clean_order \
	.map(lambda l: (tuple(l[1]),([(l[0])]))) \
	.reduceByKey(lambda x, y: x+y).filter(lambda l: len(l[1]) >= 2) \
	.map(lambda l: (l[1], l[0][0], l[0][1], l[0][2])) \
	.sortBy(lambda a: a[1])


stats.coalesce(1).saveAsTextFile("file:///Users/alessio/Documents/Projects/bd_first_project/results/job3/spark/")
