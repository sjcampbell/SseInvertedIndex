#!/bin/sh

echo "Executing query for: $1"

java -cp target/classes:target/bigdata-0.1.0-SNAPSHOT.jar com.sjcampbell.spark.sse.retrieval.BooleanRetrieval --index indexOutput --data-path data/Shakespeare.txt --query $1