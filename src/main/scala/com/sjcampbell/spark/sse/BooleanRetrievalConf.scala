package com.sjcampbell.spark.sse

import org.rogach.scallop._

class BooleanRetrievalConf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(index, query)
    val index = opt[String](descr = "index path", required = true)
    val query = opt[String](descr = "query", required = true)
}