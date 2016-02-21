package com.sjcampbell.spark.sse

import java.util.StringTokenizer
import scala.collection.JavaConverters._

/*
 * Author: Jimmy Lin
 * URL: https://github.com/lintool/bespin/blob/master/src/main/scala/io/bespin/scala/util/Tokenizer.scala
 * Thanks!
 */
trait Tokenizer {
  def tokenize(s: String): List[String] = {
    new StringTokenizer(s).asScala.toList
      .map(_.asInstanceOf[String].toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", ""))
      .filter(_.length != 0)
  }
}