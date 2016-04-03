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
        	    System.out.println(docId + "\t" + line);
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
        var i = 0
        var searchCount = 0
	    var docId = new LongWritable()
        var continueSearching = true
        
        while(continueSearching) {
            continueSearching = false
            
            wordKey = IndexEncryptor.GenerateWordKey(word)
            labelBytes = IndexEncryptor.EncryptWord(word, wordKey, searchCount)
            key.set(labelBytes)
            
            for (reader: MapFile.Reader <- indexReaders) {
                println("Searching map file " + i)
    	        i += 1
    	        
    	    	val result = reader.get(key, docId);
    	    	if (result != null) {
    	    	    
    	    	    println("docId is found: " + docId)
    	    	    println("docId value found: " + docId.get())
    	    	    
    	    		val docIdLong = docId.get()
    	    	    if (docIdLong > 0) {
    	    	        println("Found docID: " + docIdLong)
    	    	        postings = postings += docIdLong
        	         
    	    	        // We've found a document ID for this label, so increment count and search next. 
    	    	        continueSearching = true
    	    	        searchCount += 1
    	    	    }
    	    	}
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
