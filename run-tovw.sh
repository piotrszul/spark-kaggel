#!/bin/bash
set -e
hadoop fs -rm -r -f kaggel/sample_6p_1_20p.vw
spark-submit --master yarn-client --num-executors 48 --driver-memory 4G --executor-memory 6G  --class df.DataFrameApp  target/scala-2.10/spark-kaggel_2.10-1.0.jar kaggel/sample_6p_1_20p.csv kaggel/sample_6p_1_20p.vw 0 

