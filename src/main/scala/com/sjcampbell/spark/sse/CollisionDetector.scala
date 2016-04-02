package com.sjcampbell.spark.sse

import org.apache.log4j._
import org.apache.hadoop.io._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{ Job => NewHadoopJob }
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner


object CollisionDetector extends Tokenizer{
    val log = Logger.getLogger(getClass().getName())
    
    // Partition based on term 
	class HashPartitioner(numParts: Int) extends Partitioner {
		def numPartitions: Int = numParts
		def getPartition(key: Any): Int = {
			(key.hashCode() & Int.MaxValue) % numParts
        }
	}

    class TempPartitioner(numParts: Int) extends Partitioner {
		def numPartitions: Int = numParts
		def getPartition(key: Any): Int = {
		    val encWordPair = key.asInstanceOf[(String, String)]
			(encWordPair._1.hashCode() & Int.MaxValue) % numParts
        }
	}
    
    def main(argv: Array[String]) {
        val args = new Conf(argv)
        
        log.info("Input: " + args.input())
        log.info("Output: " + args.output())
        log.info("Number of reducers: " + args.reducers())
        
        val conf = new SparkConf().setAppName("Build Encrypted Inverted Index")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val sc = new SparkContext(conf)
        sc.setJobDescription("Building and encrypted inverted index using a secure searchable encryption scheme.")
        
        val outputDir = new Path(args.output())
        FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
        
        // Get RDD in hadoop file so we have line offsets to use as document IDs
        val inputFile = args.input()
        val hadoopConfig = new NewHadoopJob().getConfiguration
        val data = sc.newAPIHadoopFile (inputFile, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], hadoopConfig)
        
        var termDocIds = data.flatMap  {
            case (docId, line) => {
                val tokens = tokenize(line.toString())
                tokens.distinct.map(term => 
                    { 
                        // Get pairs of (word, docID)
                        (term, docId)
                    })
            }
        }

        // Get keys all together and loop through, one key at a time, outputting a compressed document postings list for each.
        val encryptedWordDocIds = termDocIds.repartitionAndSortWithinPartitions(new HashPartitioner(args.reducers()))
        
        // Replace search words with encrypted versions of the word
        .mapPartitions { 
            case (iter) => {
                var currentWord = ""
                var currentIndex = 0
                var wordKey = Array[Byte]()
                
                iter.map {
                    case (word, docId) => {
                        if (word != currentWord) {
                            // Reset word and index
                            currentIndex = 0
                            currentWord = word
                            wordKey = IndexEncryptor.GenerateWordKey(word)
                        }

                        // Generate encrypted label
                        val encryptedLabel = IndexEncryptor.EncryptWord(word, wordKey, currentIndex)
                        currentIndex = currentIndex + 1
                        
                        // Get label into hex format so that the output is readable. 
                        val hexLabel = encryptedLabel.map("%02x".format(_)).mkString("")
                        (hexLabel, (word, docId.toString(), 1))
                    }
                }
            }
        }

        // Reduce by key to save output file with collisions that contain ';' characters.
        .reduceByKey {
            case (tuple1, tuple2) => {
                log.warn("!Collision Found! Word1: " + tuple1._1 + " DocID1: " + tuple1._2 + " Word2: " + tuple2._1 + " DocID2: " + tuple2._2)
                (tuple1._1 + "; " + tuple2._1, tuple1._2 + "; " + tuple2._2, tuple1._3 + tuple2._3)
            }
        }
        .saveAsTextFile(args.output())
    }
}