#!/bin/bash
spark-submit --driver-memory 2g --class com.sjcampbell.spark.sse.BuildInvertedIndex target/bigdata-0.1.0-SNAPSHOT.jar --input data/Shakespeare.txt --output indexOutput