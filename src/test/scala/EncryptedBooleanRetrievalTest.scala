package com.sjcampbell.spark.sse

import org.scalatest.FunSuite
import org.apache.hadoop.io._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.MapFile
import org.apache.hadoop.fs._

class EncryptedBooleanRetrievalTest extends FunSuite{
    test("Retrieve all document IDs for a keyword") {
        val word = "Hellespont"
        val label = IndexEncryptor.EncryptWord(word, null, 0)
        
        val fs = FileSystem.get(new Configuration());
        
        val filePath = new Path("indexOutput")
        val reader = new MapFile.Reader(filePath, fs.getConf())
        
        val key = new Text("test")
        val docId = new LongWritable()
        val result = reader.get(key, docId)
        
        if (result != null) {
            val longResult = result.asInstanceOf[LongWritable]
            val resultDocId = longResult.get()
        }
        else {
            println("Didn't find anything for the query key")
        }
    }
}