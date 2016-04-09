package com.sjcampbell.spark.sse

import org.apache.log4j._
import org.apache.hadoop.io._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{ Job => NewHadoopJob }
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import tl.lin.data.array.LongArrayWritable

object BuildInvertedIndex extends Tokenizer{
    val log = Logger.getLogger(getClass().getName())
    
    class WordPartitioner(numParts: Int) extends Partitioner {
		def numPartitions: Int = numParts
		def getPartition(key: Any): Int = {
		    val encWordPair = key.asInstanceOf[ByteArrayStringPair]
			(encWordPair.word.hashCode() & Int.MaxValue) % numParts
        }
	}

    implicit val textOrdering = new Ordering[Text] {
        override def compare(a: Text, b: Text) = a.compareTo(b.getBytes, 0, b.getLength)
    }
    
    def main(argv: Array[String]) {
        val args = new Conf(argv)
        
        log.info("Input: " + args.input())
        log.info("Output: " + args.output())
        log.info("Number of reducers: " + args.reducers())
        log.info("Document group size: " + args.docGroupSize())
        BuildInvertedIndexTransformations.documentGroupSize = args.docGroupSize()
        
        val conf = new SparkConf().setAppName("Build Encrypted Inverted Index")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        val sc = new SparkContext(conf)
        sc.setJobDescription("Building and encrypted inverted index using a secure searchable encryption scheme.")
        
        log.info("! Deleting output file before running the job: " + args.output())
        val outputDir = new Path(args.output())
        FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
        log.info("! Output file deleted.")
        
        // Create the RDD based on a Hadoop file so each line offset is a document ID and each line is the document content.
        val inputFile = args.input()
        val hadoopConfig = new NewHadoopJob().getConfiguration
        val data = sc.newAPIHadoopFile (inputFile, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], hadoopConfig)

        // Start building the index
        log.info("! Commencing encrypted index build...")
        val docGroupSize = args.docGroupSize()
        val termDocIds = data.flatMap(BuildInvertedIndexTransformations.parseWordDocIdPairList)
        val groupedDocIds = termDocIds.reduceByKey(BuildInvertedIndexTransformations.reducePostingsLists)
        val encryptedWords = groupedDocIds.flatMap(BuildInvertedIndexTransformations.encryptWords)
         
        // There may be collisions in the hashed labels
        // Deal with duplicate hashed labels (cryptographic hash collisions) 
        // sort by label (hash) and find them.
        var collisionCount = sc.accumulator(0)
        val collisions = encryptedWords.repartitionAndSortWithinPartitions(new WordPartitioner(args.reducers()))
        .mapPartitions { 
            case (iter) => {
                val TEXT = new Text()
                var currentLabel = Array[Byte]()
                var duplicateCount = 0

                iter.flatMap {
                    case (hexLabelWordPair, docId) => {
                        if (hexLabelWordPair.encryptedWord.sameElements(currentLabel)) {
                            // TODO: Oh, your hash function generated a collision? Maybe you should get a better hash function.
                            log.info("Collision found! Word: " + hexLabelWordPair.word)
                            collisionCount += 1
                            List()
                        }
                        else {
                            duplicateCount = 0
                            currentLabel = hexLabelWordPair.encryptedWord
                            TEXT.set(hexLabelWordPair.encryptedWord)
                            List((TEXT, docId))
                        }
                    }
                }
            }
        }
        // Sort to ensure history independence - a security property of the encrypted index algorithm.
        .sortByKey()
        .saveAsNewAPIHadoopFile(args.output(), classOf[Text], classOf[LongArrayWritable], classOf[MapFileOutputFormat])
        
        log.info("Collisions found in index: " + collisionCount.value)
        
        log.info("! Index building: complete.")
    }
}

object BuildInvertedIndexTransformations extends Tokenizer {
    
    var documentGroupSize: Int = 1;
    
    implicit val textOrdering = new Ordering[Text] {
        override def compare(a: Text, b: Text) = a.compareTo(b.getBytes, 0, b.getLength)
    }
    
    def parseWordDocIdPairList(pair: (LongWritable, Text)) : TraversableOnce[(String, List[LongWritable])] = {
        // document ID:  pair._1
        // line:         pair._2
        val tokens = tokenize(pair._2.toString())
        tokens.distinct.map(word => 
            { 
                // Get pairs of (word, docID)
                // Note: Without a deep copy here, the resulting postings lists all have the same
                // value, as if the pair object reference does not change between flatmap iterations.
                val docId = new LongWritable(pair._1.get()) 
                (word, List(docId))
            })
    }
    
    def reducePostingsLists(postings1: List[LongWritable], postings2: List[LongWritable]): List[LongWritable] = {
        postings1 ++ postings2
    }

    // Encrypts words and attaches document postings lists in group sizes set by documentGroupSize 
    def encryptWords(wordDocPostings: (String, List[LongWritable])) : List[(ByteArrayStringPair, LongArrayWritable)] = {
        var encryptedPostings = List[(ByteArrayStringPair, LongArrayWritable)]()
        var wordKey = IndexEncryptor.GenerateWordKey(wordDocPostings._1)
        
        var i = 0
        while(i < wordDocPostings._2.length - 1) {
            // Encrypt the word using the private key and an integer that increments by one each time through this loop.
            val encryptedLabel = IndexEncryptor.EncryptWord(wordDocPostings._1, wordKey, i/documentGroupSize)
            val groupSize = Math.min(documentGroupSize, (wordDocPostings._2.length - i))
            
            // Initialize array to full document group size so when it's encrypted, you can't tell if it is one document ID or more.
            val ar = new Array[Long](documentGroupSize)
            for (j <- 0 to (documentGroupSize - 1)) {
                if (j > groupSize - 1) ar(j) = -1
                else ar(j) = wordDocPostings._2(i + j).get()
            }
            
            val docArray = new LongArrayWritable(ar)
            encryptedPostings = (new ByteArrayStringPair(encryptedLabel, wordDocPostings._1), docArray) +: encryptedPostings
            i += groupSize
        }
        
        encryptedPostings
    }
}