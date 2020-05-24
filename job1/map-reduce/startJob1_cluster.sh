#!/bin/bash

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
		-D stream.num.map.output.key.fields=2 \
		-D mapreduce.job.reduces=1 \
		-files Mapper.py,Reducer.py \
		-mapper Mapper.py \
		-reducer Reducer.py \
		-input input/s3/historical_stock_prices_half.csv \
		-output output
        


