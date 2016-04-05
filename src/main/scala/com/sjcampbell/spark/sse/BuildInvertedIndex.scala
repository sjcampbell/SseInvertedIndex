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
        
        val conf = new SparkConf().setAppName("Build Encrypted Inverted Index")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        val sc = new SparkContext(conf)
        sc.setJobDescription("Building and encrypted inverted index using a secure searchable encryption scheme.")
        
        val outputDir = new Path(args.output())
        FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
        
        // Create the RDD based on a Hadoop file so each line offset is a document ID and each line is the document content.
        val inputFile = args.input()
        val hadoopConfig = new NewHadoopJob().getConfiguration
        val data = sc.newAPIHadoopFile (inputFile, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], hadoopConfig)

        // Start building the index
        val termDocIds = data.flatMap(BuildInvertedIndexTransformations.parseWordDocIdPairList)
        val groupedDocIds = termDocIds.reduceByKey(BuildInvertedIndexTransformations.reducePostingsLists)
        val encryptedWords = groupedDocIds.flatMap(BuildInvertedIndexTransformations.encryptWords)
        
        // There may be collisions in the hashed labels
        // Deal with duplicate hashed labels (cryptographic hash collisions) - sort by label (hash) and find them.
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
        // Sort to ensure history independence, as security property of the encrypted index algorithm.
        .sortByKey()
        .saveAsNewAPIHadoopFile(args.output(), classOf[Text], classOf[LongWritable], classOf[MapFileOutputFormat])
    }
}

object BuildInvertedIndexTransformations extends Tokenizer {
    
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
    
    def encryptWords(wordDocPostings: (String, List[LongWritable])) : Iterable[(ByteArrayStringPair, LongWritable)] = {
        var encryptedPostings = List[(ByteArrayStringPair, LongWritable)]()
        var wordKey = IndexEncryptor.GenerateWordKey(wordDocPostings._1)
        
        val i = 0
        for(i <- 0 to (wordDocPostings._2.length - 1)) {

            // Generate encrypted label
            val encryptedLabel = IndexEncryptor.EncryptWord(wordDocPostings._1, wordKey, i)
            
            // Output plainText along with encrypted label in order to deal with collisions in encrypted labels
            // Output key must be sortable in order to use Spark's sorting/shuffle machinery.
            encryptedPostings = (new ByteArrayStringPair(encryptedLabel, wordDocPostings._1), wordDocPostings._2(i)) +: encryptedPostings                
        }
        
        encryptedPostings
    }
}