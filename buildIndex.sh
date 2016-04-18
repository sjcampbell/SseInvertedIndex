#!/bin/bash
my-spark-submit --driver-memory 6g --class com.sjcampbell.spark.sse.BuildInvertedIndex target/bigdata-0.1.0-SNAPSHOT.jar --input $1 --output $2 --doc-group-size $3 --reducers 4
