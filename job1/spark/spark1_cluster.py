from pyspark import SparkConf, SparkContext, StorageLevel

conf = SparkConf().setMaster("local[*]").setAppName("Job1")
sc = SparkContext(conf=conf)


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

input = sc.textFile("file:///home/hadoop/datasets/historical_stock_prices.csv") \
		.map(lambda linea: linea.split(","))

input = input.filter(lambda linea: linea[0] != "ticker")

input = input.filter(lambda linea: linea[7][0:4] >= "2008" and linea[7][0:4] <= "2018")

# persist RDD in memory
input.persist(StorageLevel.MEMORY_AND_DISK)

ticker_close_minimo = input.map(lambda linea: (linea[0], float(linea[4]))) \
					  .reduceByKey(lambda x, y: min(x, y))

ticker_close_massimo = input.map(lambda linea: (linea[0], float(linea[5]))) \
					   .reduceByKey(lambda x, y: max(x, y))

ticker_volume_medio = input.map(lambda linea: (linea[0], (float(linea[6]), 1))) \
	.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])) \
	.map(lambda linea: (linea[0], (linea[1][0]/linea[1][1])))

minimo_close_assoluto = input.map(lambda linea: (linea[0], (linea[2], linea[7]))) \
	.reduceByKey(lambda x, y: close_minimo(x, y)) \

massimo_close_assoluto = input.map(lambda linea: (linea[0], (linea[2], linea[7]))) \
	.reduceByKey(lambda x, y: close_massimo(x, y))\

join_incremento_percentuale = minimo_close_assoluto.join(massimo_close_assoluto)

incremento_percentuale = join_incremento_percentuale \
		   .map(lambda linea: (linea[0],
		   	                  ((float(linea[1][1][0]) - float(linea[1][0][0]))/float(linea[1][0][0]))*100))

output = ticker_close_massimo.join(ticker_close_minimo) \
						.join(incremento_percentuale) \
						.join(ticker_volume_medio) \
						.map(lambda x: [x[0], x[1][0][0][0], x[1][0][0][1],
						     x[1][0][1], x[1][1]]).sortBy(lambda a: a[3], ascending=False) \
						.take(10)

sc.parallelize(output).coalesce(1) \
					  .saveAsTextFile("file:///home/hadoop/output/output_job1/")
