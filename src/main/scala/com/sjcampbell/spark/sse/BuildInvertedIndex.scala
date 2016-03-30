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


object BuildInvertedIndex extends Tokenizer{
    val log = Logger.getLogger(getClass().getName())
    
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
    
    def main(argv: Array[String]) {
        val args = new Conf(argv)
        
        log.info("Input: " + args.input())
        log.info("Output: " + args.output())
        log.info("Number of reducers: " + args.reducers())
        
        val conf = new SparkConf().setAppName("Build Encrypted Inverted Index")
        val sc = new SparkContext(conf)
        sc.setJobDescription("Building and encrypted inverted index using a secure searchable encryption scheme.")
        
        val outputDir = new Path(args.output())
        FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
        
        // RDD[String]
        val inputFile = args.input()
        val hadoopConfig = new NewHadoopJob().getConfiguration
        val data = sc.newAPIHadoopFile (inputFile, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], hadoopConfig)

        // Use Pseudo-random function to create encrypted labels for each word.
        def encryptWord(word: String, wordKey: Array[Byte], docCount: Int) : Array[Byte] = {
            val plainText = word + docCount.toString()
            IndexEncryptor.KeyedCryptoHash(plainText, wordKey, docCount)
        }
        
        
        /* Getting term/docID pairs 
         * ========================
         * 
         * Option 1 (Try this first) 
         * Map, sort, then loop through sorted, resetting encrypted count when encountering a new word.
         * 
         * Option 2
         * Map, then reduce into [term, List<DocID>], then cycle through those, writing each pair
         */

        /* Encryption
         * ==========
         * 
         * Option 1 (Try this first)
         * Deal with collisions using bucket hash. Store multiple docIDs with encrypted term,
         * then when fetching, try to decrypt all of them and match the first number to the docId sequence
         * number. If it's a match, then use it. Otherwise don't. 
         * The only problem with this is that some of the encrypted keys will have a slightly larger encrypted 
         * postings list, rather than single key/pair postings.
         * 
         * Option 2 
         * Encrypt all terms with a large-enough crypto hash that the odds of collisions will be low.
         * 
         * Option 3
         * Attempt cuckoo hash? 
         */
        
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
                            // Reset term and index
                            currentIndex = 0
                            currentWord = word
                            wordKey = IndexEncryptor.GenerateWordKey(word)
                        }

                        // Generate encrypted term
                        val encryptedLabel = encryptWord(word, wordKey, currentIndex)
                        currentIndex = currentIndex + 1
                        
                        // Output plainText along with encrypted label in order to deal with collisions in encrypted labels
                        // Output key must be sortable in order to use Spark's sorting/shuffle machinery.
                        (new ByteArrayStringPair(encryptedLabel, word), docId)
                    }
                }
            }
        }
        // Outputs RDD[String, LongWritable]
        
        // There may be collisions in the encrypted labels
        // Deal with duplicate encrypted terms (cryptographic hash collisions)
        .repartitionAndSortWithinPartitions(new WordPartitioner(args.reducers()))
        
        // TODO NEXT:Now that we have encrypted words and their unencrypted counterparts, find out what kind of collisions
        // we've built up.
               
        
        // Once all is done, ensure index is stored with history independence. This might mean another random sort. 
        // Sorting only within partitions should probably add enough chaos to the order.
        //.repartitionAndSortWithinPartitions(new HashPartitioner(args.reducers()))
    }
}