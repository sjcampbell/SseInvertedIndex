package com.sjcampbell.spark.sse

object BooleanRetrieval extends App {
    var appArgs = new BooleanRetrievalConf(args)
    val query = appArgs.query()
    val indexFilePath = appArgs.index()
    
    // Load mapfile and execute query.
}