package com.sjcampbell.spark.sse.retrieval

import org.rogach.scallop._

class BooleanRetrievalConf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(index, query)
    val index = opt[String](descr = "Index path", required = true)
    val dataPath = opt[String](descr = "Data file path", required = true)
    val query = opt[String](descr = "Query", required = true)
}