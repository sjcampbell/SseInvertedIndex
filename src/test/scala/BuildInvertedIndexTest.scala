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
    
    /* These tests were written for the transformations, but no longer work since the transformations
     * have been moved into anonymous functions due to the use of the document group size variable, since
     * it has to be used within flatmap function. That led to a bunch of fallout, which eventually led to
     * just putting all the transformation functions in the "main" function.
     * 

    test("encryptWords should return document IDs split up by document group size") {
        val indexBuilder = new EncryptedIndexBuilder(4)
        val docIds = List(new LongWritable(1L), new LongWritable(2L), new LongWritable(3L), new LongWritable(4L), 
                new LongWritable(5L), new LongWritable(6L), new LongWritable(7L), new LongWritable(8L))
        val wordDocIds = ("test", docIds)
        
        println("Encrypting words")
        val result = indexBuilder.encryptWords(wordDocIds)
        
        assert(result.size == 2)
    }
    
    test("encryptWords should group document IDs into defined group sizes.") {
        val testGroupSize = 4
        val indexBuilder = new EncryptedIndexBuilder(testGroupSize)
        val docIds = List(new LongWritable(1L), new LongWritable(2L), new LongWritable(3L), new LongWritable(4L), 
                new LongWritable(5L), new LongWritable(6L))
        val wordDocIds = ("test", docIds)
        
        val result = indexBuilder.encryptWords(wordDocIds)
        
        assert(result.size == 2)

        println("Result: " + result)
        
        // The first iterator element is the last group prepended by the index builder, so it's the one with fewer elements.
        assert(result(0)._2.size() == testGroupSize)
        assert(result(1)._2.size() == testGroupSize)
        result.iterator
    }*/
    
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