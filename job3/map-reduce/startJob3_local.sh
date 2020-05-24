#!/bin/bash

mapred streaming \
	  -D stream.num.map.output.key.fields=3 \
		-D mapreduce.partition.keypartitioner.options=-k1,1 \
		-D mapreduce.job.reduces=2 \
		-files FirstMapper.py,FirstReducer.py,../../contents/historical_stocks.csv \
		-mapper FirstMapper.py \
		-reducer FirstReducer.py \
		-input /user/alessio/input/historical_stock_prices_half.csv \
		-output /user/alessio/output/job3_hadoop_tmp \
		-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
&& 	\
mapred streaming \
		-D stream.num.map.output.key.fields=3 \
		-D mapreduce.job.reduces=1 \
		-files SecondMapper.py,SecondReducer.py \
		-mapper SecondMapper.py \
		-reducer SecondReducer.py \
		-input /user/alessio/output/job3_hadoop_tmp/part-* \
		-output /user/alessio/output/result_job3 \
