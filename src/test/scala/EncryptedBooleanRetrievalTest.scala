package com.sjcampbell.spark.sse

import org.scalatest.FunSuite
import org.apache.hadoop.io.Text

class EncryptedBooleanRetrievalTest extends FunSuite{
    test("Retrieve all document IDs for a keyword") {
        val word = "Hellespont"
        val label = IndexEncryptor.EncryptWord(word, null, 0)
        
        // TODO: Execute search.
    }
}