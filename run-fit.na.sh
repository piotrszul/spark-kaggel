#!/bin/bash
set -e
spark-submit --master yarn-client --num-executors 32  --class learn.FitNa  target/scala-2.10/spark-kaggel_2.10-1.0.jar kaggel/test_with_label kaggel/test_nona.csv

