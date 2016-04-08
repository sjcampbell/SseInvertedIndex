package com.sjcampbell.spark.sse.retrieval

import scala.collection.mutable.HashSet

import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.Set
import java.util.Stack

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.PathFilter
import org.apache.hadoop.io.MapFile
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.LongWritable
import com.sjcampbell.spark.sse.IndexEncryptor
import tl.lin.data.array.LongArrayWritable

object BooleanRetrieval {
    def main(argv: Array[String]) {
        var appArgs = new BooleanRetrievalConf(argv)
        val fs = FileSystem.get(new Configuration());
    
        // Load mapfile and execute query.
        val booleanRetriever = new BooleanRetriever(appArgs.index(), appArgs.dataPath(), fs)
        booleanRetriever.executeQuery(appArgs.query())    
    }    
}

class BooleanRetriever(indexPath: String, dataPath: String, fs: FileSystem) {
    
    val docGroupSize = 4
    
    val postings = new Stack[HashSet[Long]]()
    var indexReaders = List[MapFile.Reader]()
    val collection = fs.open(new Path(dataPath));
    
    // Initialize map file readers
    val dirPath = new java.io.File(indexPath)
    if (!dirPath.exists()) {
        throw new Exception("Index directory was not found.")
    }

    val fileList = fs.listStatus(new Path(indexPath), new PartPathFilter())
    fileList.foreach { 
        case (file) => {
            indexReaders = indexReaders :+ new MapFile.Reader(file.getPath(), fs.getConf())
        }
    }

    if (indexReaders.length == 0) {
        throw new Exception("No 'part' files were found in index path: " + indexPath)
    }
   
    
    def executeQuery(query: String) = {
        val words = query.toLowerCase().split("\\s+");

        println("Query words: " + words.mkString(" "))
        
        words.foreach(word => { 
            if (word.equals("AND")) {
                performAND();
            } 
            else if (word.equals("OR")) {
                performOR();
            } 
            else {
                println("Pushing word: " + word)
                pushWordPostings(word);
            }
        })
    
        val set = postings.pop()
        
        println("Postings list set size: " + set.size)
        println("")
        println("** Results for query: " + query + " **")
        println("")
        
        val iter = set.iterator
        while(iter.hasNext) {
        	val docId = iter.next();
        	
        	println("Retrieving doc ID: " + docId)
        	
        	if (docId > 0) {
        	    val line = fetchLine(docId);
        	    val subLine = if (line.length > 80) line.substring(0, 80) + "..." else line;
        	    System.out.println(docId + "\t" + subLine);
        	}
        }
    }
    
    def fetchLine(offset: Long) = {
        collection.seek(offset)
        val reader = new BufferedReader(new InputStreamReader(collection))
        reader.readLine()
    }
    
    def performAND() {
        
    }
    
    def performOR() {
        
    }
    
    def pushWordPostings(word: String) {
        postings.push(fetchDocPostings(word))
    }
    
    def fetchDocPostings(word: String) : HashSet[Long] = {
        var postings = new HashSet[Long]()
        
        val key = new Text();
        var wordKey = Array[Byte]()
        var labelBytes = Array[Byte]()

        println("Searching map files for key " + word)
        var searchCount = 0
        var continueSearching = true
        
        while(continueSearching) {
            continueSearching = false
            
            wordKey = IndexEncryptor.GenerateWordKey(word)
            labelBytes = IndexEncryptor.EncryptWord(word, wordKey, searchCount)
            key.set(labelBytes)

            var docIdArray = new LongArrayWritable()            

            var i = 0
            // Loop through map files to search for key. 
            // TODO: There's probably a more efficient way to do this since values should be separated
            // between MapFile partitions by an ordered key.
            while(i < indexReaders.length) {
                println("Searching map file " + i)
    	        
    	    	val result = indexReaders(i).get(key, docIdArray);
    	    	if (docIdArray.size() > 0) {
    	    	    
    	    	    println("docIdArray is found. Size: " + docIdArray.size())
    	    	    for (k <- 0 to docIdArray.size() - 1) {
    	    	        println("DocId value found: " + docIdArray.get(k))
    	    	        postings = postings += docIdArray.get(k)
    	    	    }
    	    	    
    	    	    // We've found a document ID for this label, so increment count and search next.
        	        if (docIdArray.size() >= docGroupSize) {
        	            continueSearching = true
        	            searchCount += 1
        	        }
        	        
        	        // Since we've found the word we were searching for, break this god-forsaken MapFile search loop
        	        i += indexReaders.length
    	    	}
    	    	
    	    	i += 1
    	    }
        }

        if (postings.size == 0) {
            println("No postings found for search word: " + word)
        }
        
        postings
    }
    
    class PartPathFilter extends PathFilter {
        def accept(path: Path): Boolean = {
            path.getName().matches("part-r?-?[0-9]+")
        }
    }
}
