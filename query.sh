#!/bin/sh

echo "Executing query for: $3"

hadoop jar target/bigdata-0.1.0-SNAPSHOT.jar com.sjcampbell.spark.sse.retrieval.BooleanRetrievalJava\
	      -index $1 \
		  -collection $2 \
		  -query $3
