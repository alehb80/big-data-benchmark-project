#!/bin/bash

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
				-D stream.num.map.output.key.fields=3 \
				-D mapreduce.partition.keypartitioner.options=-k1,1 \
				-D mapreduce.job.reduces=2 \
				-files FirstMapper.py,FirstReducer.py \
				-mapper FirstMapper.py \
				-reducer FirstReducer.py \
				-input /input/s3/historical_stock_prices.csv \
				-output output/job3_hadoop_tmp/ \
				-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
&& 	\
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
				-D stream.num.map.output.key.fields=3 \
				-D mapreduce.job.reduces=1 \
				-files SecondMapper.py,SecondReducer.py \
				-mapper SecondMapper.py \
				-reducer SecondReducer.py \
				-input /output/job3_hadoop_tmp/part-* \
				-output /output/result_final
