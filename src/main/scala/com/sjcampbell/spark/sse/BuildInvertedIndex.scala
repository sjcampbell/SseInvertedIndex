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

        def encryptTerm(term: String, docCount: Int) : String = {
            val plainText = term + docCount.toString()
            
            // TODO: find cryptographic hash function for this.
            
            return plainText;
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
        .repartitionAndSortWithinPartitions(new HashPartitioner(args.reducers()))

        // Replace search words with encrypted versions of the word
        .mapPartitions { 
            case (iter) => {
                var currentTerm = ""
                var currentIndex = 0
                
                iter.map {
                    case (term, docId) => {
                        if (term != currentTerm) {
                            // Reset term and index
                            currentIndex = 0
                            currentTerm = term
                        }

                        // Generate encrypted term
                        val encryptedTerm = encryptTerm(term, currentIndex)
                        currentIndex = currentIndex + 1
                        (encryptedTerm, docId)                           
                    }
                }
            }
        }
        // Outputs RDD[String, LongWritable]

        // Deal with duplicate encrypted terms (cryptographic hash collisions)
        /*.reduceByKey {
            case (docId1: LongWritable, docId2: LongWritable) => {
                docId1 + docId2
            }
        }*/
        
        // Once encrypted, sort again, but by encrypted term to gain history independence of the index.
        //.repartitionAndSortWithinPartitions(new HashPartitioner(args.reducers()))
    }
}