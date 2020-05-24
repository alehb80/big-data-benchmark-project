#!/bin/bash

# try with -D mapreduce.job.reduces=1

mapred streaming \
   -D stream.num.map.output.key.fields=3 \
   -D mapreduce.partition.keypartitioner.options=-k1,1 \
   -D mapreduce.job.reduces=2 \
   -files Mapper.py,Reducer.py,../../contents/historical_stocks.csv \
   -mapper Mapper.py \
   -reducer Reducer.py \
   -input /user/alessio/input/historical_stock_prices_half.csv \
   -output /user/alessio/output/result_job2 \
   -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner