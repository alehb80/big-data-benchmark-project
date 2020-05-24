from pyspark import SparkConf, SparkContext, StorageLevel

conf = SparkConf().setMaster("local[*]").setAppName("Job1")
sc = SparkContext(conf=conf)


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


input = sc.textFile("file:///Users/alessio/Documents/Projects/bd_first_project/contents/historical_stock_prices.csv") \
		.map(lambda l: l.split(","))

input = input.filter(lambda l: l[0] != "ticker")

input = input.filter(lambda l: "2008" <= l[7][0:4] <= "2018")

# persist RDD in memory
input.persist(StorageLevel.MEMORY_AND_DISK)

low_min_tckr = input.map(lambda l: (l[0], float(l[4]))).reduceByKey(lambda x, y: min(x, y))

high_max_tckr = input.map(lambda l: (l[0], float(l[5]))).reduceByKey(lambda x, y: max(x, y))

mean_volume_tckr = input.map(lambda l: (l[0], (float(l[6]), 1))) \
	.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])) \
	.map(lambda l: (l[0], (l[1][0]/l[1][1])))

min_close_total = input.map(lambda l: (l[0], (l[2], l[7]))) \
	.reduceByKey(lambda x, y: close_price_min(x, y)) \

max_close_total = input.map(lambda l: (l[0], (l[2], l[7]))) \
	.reduceByKey(lambda x, y: close_price_max(x, y))\

join_price_variation = min_close_total.join(max_close_total)

price_variation = join_price_variation \
	.map(lambda l: (l[0], ((float(l[1][1][0]) - float(l[1][0][0]))/float(l[1][0][0]))*100))

stats = high_max_tckr.join(low_min_tckr) \
	.join(price_variation) \
	.join(mean_volume_tckr) \
	.map(lambda x: [x[0], x[1][0][0][0], x[1][0][0][1], x[1][0][1], x[1][1]]) \
	.sortBy(lambda a: a[3], ascending=False) \
	.take(10)

sc.parallelize(stats).coalesce(1) \
	.saveAsTextFile("file:///Users/alessio/Documents/Projects/bd_first_project/results/job1/spark/")
