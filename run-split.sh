#!/bin/bash
set -e
spark-submit --master yarn-client --num-executors 32  --class learn.KaggelSplit  target/scala-2.10/spark-kaggel_2.10-1.0.jar kaggel/train.vw

