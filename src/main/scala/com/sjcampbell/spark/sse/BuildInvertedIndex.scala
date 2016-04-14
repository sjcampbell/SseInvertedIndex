package com.sjcampbell.spark.sse

import org.apache.log4j._
import org.apache.hadoop.io._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{ Job => NewHadoopJob }
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import tl.lin.data.array.LongArrayWritable

//import tl.lin.data.pair.PairOfStringInt
import com.sjcampbell.spark.sse.utils.PairOfStringInt

object BuildInvertedIndex extends Tokenizer{
    val log = Logger.getLogger(getClass().getName())
    
    implicit val textOrdering = new Ordering[Text] {
        override def compare(a: Text, b: Text) = a.compareTo(b.getBytes, 0, b.getLength)
    }
    
    implicit val stringIntOrdering = new Ordering[PairOfStringInt] {
        override def compare(pair1: PairOfStringInt, pair2: PairOfStringInt) = {
            pair1.compareTo(pair2)
        }
    }
    
    def main(argv: Array[String]) {
        val args = new Conf(argv)
        
        log.info("Input: " + args.input())
        log.info("Output: " + args.output())
        log.info("Number of reducers: " + args.reducers())
        log.info("Document group size: " + args.docGroupSize())

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
        // ========================
        log.info("! Commencing encrypted index build...")
        
        val termDocIds = data.flatMap(InvertedIndexTransformations.parseWordDocIdPairs)

        // Group like words together then loop through, one key at a time, outputting document IDs with encrypted labels.
        // This could use groupBy, but by sorting, future us could implement gap compression + variable-length integers.
        val sortedWordDocIds = termDocIds.repartitionAndSortWithinPartitions(new InvertedIndexTransformations.HashPartitioner(args.reducers()))

        val bcGroupSize = sc.broadcast(args.docGroupSize())
        val groupSize = args.docGroupSize()
        
        val assignedDocGroupings = sortedWordDocIds.mapPartitions {
            case (iter) => {
                
                var currentWord = ""
                var count = 0
                
                iter.map {
                    case (word, docId) => {
                        if (word != currentWord) {
                            currentWord = word
                            count = 0
                        }
                        
                        count += 1
                        
                        val pl = Array[Long](docId.get())
                        (new PairOfStringInt(word, Math.floor((count - 1)/groupSize).toInt), new LongArrayWritable(pl))
                    }
                }
            }
        }

        val groupedPostingsLists = assignedDocGroupings.reduceByKey(InvertedIndexTransformations.reduceLongArrays)
        val encryptedWords = InvertedIndexTransformations.encryptGroupedPostings(groupedPostingsLists, groupSize)
        
        var collisionCount = sc.accumulator(0)
        
        // Sort to ensure history independence - a security property of the encrypted index algorithm.
        val collisions = encryptedWords.repartitionAndSortWithinPartitions(new InvertedIndexTransformations.WordPartitioner(args.reducers()))
        .mapPartitions { 
            case (iter) => {
                val TEXT = new Text()
                var currentLabel = Array[Byte]()
                var duplicateCount = 0

                iter.flatMap {
                    case (hexLabelWordPair, docId) => {
                        if (hexLabelWordPair.encryptedWord.sameElements(currentLabel)) {
                            // TODO: Oh, your hash function generated a collision? Maybe you should get a better hash function.
                            log.info("! Collision found! Word: " + hexLabelWordPair.word)
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
        // Keys have to be in order to store as a MapFile
        .sortByKey()
        .saveAsNewAPIHadoopFile(args.output(), classOf[Text], classOf[LongArrayWritable], classOf[MapFileOutputFormat])
        
        log.warn("! Collisions found in index: " + collisionCount.value)
        log.info("! Index building: complete.")
    }
}

object InvertedIndexTransformations extends Tokenizer {
    // Partition based on term 
	class HashPartitioner(numParts: Int) extends Partitioner {
		def numPartitions: Int = numParts
		def getPartition(key: Any): Int = {
			(key.hashCode() & Int.MaxValue) % numParts
        }
	}
    
    class WordPartitioner(numParts: Int) extends Partitioner {
		def numPartitions: Int = numParts
		def getPartition(key: Any): Int = {
		    val encWordPair = key.asInstanceOf[ByteArrayStringPair]
			(encWordPair.word.hashCode() & Int.MaxValue) % numParts
        }
	}
    
    def parseWordDocIdPairs(pair: (LongWritable, Text)) : TraversableOnce[(String, LongWritable)] = {
        val line = pair._2
        val docId = pair._1
        val tokens = tokenize(line.toString())
        tokens.distinct.map(word => 
            { 
                // Get pairs of (word, docID)
                (word, docId)
            })
    }
    
    def reduceLongArrays(ar1: LongArrayWritable, ar2: LongArrayWritable) = {
        val combinedArray = new Array[Long](ar1.size() + ar2.size())
        System.arraycopy(ar1.getArray(), 0, combinedArray, 0, ar1.size())
        System.arraycopy(ar2.getArray(), 0, combinedArray, ar1.size(), ar2.size())
        new LongArrayWritable(combinedArray)   
    }
    
    def encryptGroupedPostings(groupedPostings: RDD[(PairOfStringInt, LongArrayWritable)], groupSize: Int) = {
        groupedPostings.map {
            case (wordIdPostingsPair) => {
                val word = wordIdPostingsPair._1.getLeftElement()
                val index = wordIdPostingsPair._1.getRightElement()
                val wordKey = IndexEncryptor.GenerateWordKey(word)
                val encryptedLabel = IndexEncryptor.EncryptWord(word, wordKey, index)
                
                val postingsGroup = new Array[Long](groupSize)
                for (i <- 0 to wordIdPostingsPair._2.size() - 1) {
                    postingsGroup(i) = wordIdPostingsPair._2.get(i)
                }
                
                // Fill in the rest of the group (if any) with -1's. This will demonstrate the size of the output index
                // when it gets encrypted. The encrypted postings lists can't be different sizes, or else information will
                // be leaked: in particular, an attacker could approximately determine which encrypted words are uncommon 
                // and only appear in one or two documents
                for (i <- wordIdPostingsPair._2.size() to groupSize - 1) {
                    postingsGroup(i) = -1L
                }
                
                (new ByteArrayStringPair(encryptedLabel, word), new LongArrayWritable(postingsGroup))
            }
        }
    }
    
    def encryptGroupedPostingsLists(wordIdPostingsPair: (PairOfStringInt, LongArrayWritable)) = {
        val word = wordIdPostingsPair._1.getLeftElement()
        val index = wordIdPostingsPair._1.getRightElement()
        val wordKey = IndexEncryptor.GenerateWordKey(word)
        val encryptedLabel = IndexEncryptor.EncryptWord(word, wordKey, index)
        (new ByteArrayStringPair(encryptedLabel, word), wordIdPostingsPair._2)
    }
}