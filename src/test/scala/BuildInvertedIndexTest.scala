package com.sjcampbell.spark.sse

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import org.apache.hadoop.io._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
import org.apache.log4j._
import com.sjcampbell.spark.sse.utils.PairOfStringInt
import tl.lin.data.array.LongArrayWritable

class BuildInvertedIndexTest extends FunSuite with BeforeAndAfter {

    private val master = "local"
    private val appName = "encrypted-index-testing"

	private var sc: SparkContext = _
	
	private var testWordDocIds: Array[(String, LongWritable)] = _

	val log = Logger.getLogger(getClass().getName())
	
	before {
	    val conf = new SparkConf()
    			.setMaster(master)
    			.setAppName(appName)
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

		sc = new SparkContext(conf)
	    
	    testWordDocIds =
            Array(("w1", 1L), ("w1", 2L), ("w1", 3L), ("w1", 4L), 
                ("w2", 5L), ("w2", 6L),
                ("w3", 7L), ("w4", 8L),
                ("w5", 9L))
            .map {
                case (word, l) => {
                    (word, new LongWritable(l))
                }
            }
    }
    
    after {
        if (sc != null) {
            sc.stop()
        }
    }
    
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
    
    test("reduceLongArrays should combine two LongArrayWritable arrays") {
        val ar1 = Array[Long](0L, 1L, 2L)
        val larw1 = new LongArrayWritable(ar1)
        
        val ar2 = Array[Long](3L, 4L, 5L)
        val larw2 = new LongArrayWritable(ar2)

        val result = InvertedIndexTransformations.reduceLongArrays(larw1, larw2)
        
        assert(result.size() == 6)
        
        for (i <- 0 to result.size() - 1) {
            assert(result.get(i) == i.toLong)
        }
    }
    
    test("encryptWords should group document IDs by the document group size") {
        // Arrange
        val groupedPostings = Array((("w1", 0), 1L), (("w1", 0), 2L), (("w1", 1), 3L), (("w1", 1), 4L), 
                (("w2", 0), 5L), (("w2", 0), 6L),
                (("w3", 0), 7L), (("w4", 0), 8L),
                (("w5", 0), 9L))
            // convert to other types
            .map {
                case (wordIndexPair, l) => {
                    (new PairOfStringInt(wordIndexPair._1, wordIndexPair._2), new LongArrayWritable(Array[Long](l)))
                }
            }
        
        val groupedPostingsRdd = sc.parallelize(groupedPostings, 1)
        
        // Act
        val reducedPostingsRdd = groupedPostingsRdd.reduceByKey(InvertedIndexTransformations.reduceLongArrays)
        val encryptedWordRdd = InvertedIndexTransformations.encryptGroupedPostings(reducedPostingsRdd, 2)
        val result = encryptedWordRdd.collect()
        
        // Assert
        var w1Count = 0        
        result.foreach {
            case (labelWordPair, docIds) => {
                
                println("LabelWord pair: " + labelWordPair)
                println("Doc IDs: " + docIds.getArray().mkString(";"))
                
                if (labelWordPair.word == "w1") {
                    assert(docIds.size() == 2)
                    w1Count += 1
                }
                if (labelWordPair.word == "w2") assert(docIds.size() == 2)
                if (labelWordPair.word == "w3") assert(docIds.get(0) == 7L)
            }
        }
        
        assert(w1Count == 2, "w1Count did not match expected group count")
    }

    /*test("Build index with distinct labels (disregard collisions for test dataset)") {
       
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