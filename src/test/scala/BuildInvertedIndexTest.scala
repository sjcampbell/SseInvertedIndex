package com.sjcampbell.spark.sse

import org.scalatest.FunSuite
import org.apache.hadoop.io._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration

class BuildInvertedIndexTest extends FunSuite {

    test("Personal scala test to compare two different Text objects") {
        val b1 = Array[Byte](1, 1, 1)
        val b2 = Array[Byte](2, 2, 2)
        val b3 = Array[Byte](2, 2, 2)
        
        val t1 = new Text(b1)
        val t2 = new Text(b2)
        val t3 = new Text(b3)
        
        val result1 = t1.compareTo(t2.getBytes, 0, t2.getLength)
        assert(result1 == -1)
        
        val result2 = t2.compareTo(t3.getBytes, 0, t3.getLength())
        assert(result2 == 0)
    }

    
    /* This test shows that the token parser doesn't parse out words that are separated by only a comma with no spaces
    test("Tokenize") {
        val testStr = "For,she's not"
        val testStr2 = "The other, that she's in earth, from whence God send her quickly!"
        val result = TestTokenizer.tokenize(testStr2)
        //assert(result.length == 3)
    }
    
    object TestTokenizer extends Tokenizer {
        def testTokenize(str: String) : List[String] = {
            tokenize(str)
        }
    }
    */
}