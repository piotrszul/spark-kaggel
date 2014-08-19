#!/bin/bash
set -e
hadoop fs -rm -r -f kaggel/train.vw
spark-submit --master yarn-client --num-executors 48 --driver-memory 4G --executor-memory 6G  --class df.DataFrameApp  target/scala-2.10/spark-kaggel_2.10-1.0.jar kaggel/train.csv kaggel/train.vw 0 

