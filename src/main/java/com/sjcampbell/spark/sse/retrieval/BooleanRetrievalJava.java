package com.sjcampbell.spark.sse.retrieval;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Stack;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import com.sjcampbell.spark.sse.IndexEncryptor;
import tl.lin.data.array.LongArrayWritable;

public class BooleanRetrievalJava extends Configured implements Tool {
	private MapFile.Reader[] indexReaders;
	private FSDataInputStream collection;
	private Stack<ArrayList<Long>> postingsStack;
	
	private BooleanRetrievalJava() {}

	private void initialize(String indexPath, String collectionPath, FileSystem fs) throws IOException {
		initializeReader(indexPath, fs);
		collection = fs.open(new Path(collectionPath));
		postingsStack = new Stack<ArrayList<Long>>();
	}
	  
	private void initializeReader(String indexPath, FileSystem fs) throws IOException {
		FileStatus[] fileList = fs.listStatus(new Path(indexPath), 
				new PathFilter(){
			@Override public boolean accept(Path path){
				return path.getName().startsWith("part-");
			} 
		});

		indexReaders = new MapFile.Reader[fileList.length];		
		for (int i = 0; i < fileList.length; i++) {
			indexReaders[i] = new MapFile.Reader(fileList[i].getPath(), fs.getConf());
		}
	}

	private void runQuery(String query) throws IOException {
		String[] terms = query.split("\\s+");

		for (String t : terms) {
			if (t.equals("AND")) {
				performAND();
			} else if (t.equals("OR")) {
				performOR();
			} else {
				pushTermPostings(t);
			}
		}

		ArrayList<Long> postings = postingsStack.pop();
		System.out.println("Postings list set size: " + postings.size());
		System.out.println("");
		System.out.println("** Results for query: " + query + " **");
		System.out.println("");
        
		for(Long docId: postings) {
			if (docId > 0) {
        	    String line = fetchLine(docId);
        	    String subLine = line.length() > 80 ? line.substring(0, 80) + "..." : line;
        	    System.out.println(docId + "\t" + subLine);
        	}
		}
	}

	private void pushTermPostings(String term) throws IOException {
		postingsStack.push(fetchDocPostings(term));
	}

	private void performAND() throws IOException {
	}

	private void performOR() throws IOException {
	}

	private ArrayList<Long> fetchDocPostings(String term) throws IOException {
		ArrayList<Long> postings = new ArrayList<Long>();
		Text key = new Text();
		byte[] wordKey;
		byte[] labelBytes;
		int searchCount = 0;
		boolean continueSearching = true;
		
		System.out.println("Searching map files for key: " + term);
		while (continueSearching) {
			continueSearching = false;
			
			wordKey = IndexEncryptor.GenerateWordKey(term);
		    labelBytes = IndexEncryptor.EncryptWord(term, wordKey, searchCount);
		    key.set(labelBytes);
		    
		    LongArrayWritable docIdArray = new LongArrayWritable();            

		    boolean postingsListEnd = false;
            int i = 0;
            // Loop through map files to search for key.
            // TODO: There's probably a more efficient way to do this since values 
            // should be partitioned in the index.
            while(i < indexReaders.length) {
                System.out.println("Searching map file " + i);
                
                indexReaders[i].get(key, docIdArray);
    	    	if (docIdArray.size() > 0) {
    	    	    
    	    	    System.out.println("docIdArray is found. Size: " + docIdArray.size());
    	    	    for (int k = 0; k < docIdArray.size(); k++) {
    	    	    	System.out.println("DocId value found: " + docIdArray.get(k));
    	    	        postings.add(docIdArray.get(k));

    	    	        // -1's are used as filler in the index to make document lists a specific size.
    	    	        // If any are found, it means there are no more document IDs
    	    	        if (docIdArray.get(k) < 0) {
    	    	        	postingsListEnd = true;
    	    	        }
    	    	    }
    	    	    
    	    	    // We've found a document ID for this label, so increment count and search next.
        	        if (!postingsListEnd) {
        	        	searchCount += 1;
        	            continueSearching = true;
        	        }
        	        
        	        // Since we've found the word we were searching for, break the MapFile search loop
        	        // in order to begin searching for the next label.
        	        break;
    	    	}
    	    	
    	    	i += 1;
            }
		}
		
		if (postings.size() == 0) {
            System.out.println("No postings found for search word: " + term);
        }
		
		return postings;
	}

	private String fetchLine(long offset) throws IOException {
		collection.seek(offset);
		BufferedReader reader = new BufferedReader(new InputStreamReader(collection));
		return reader.readLine();
	}

	public static class Args {
		@Option(name = "-index", metaVar = "[path]", required = true, usage = "index path")
		public String index;

		@Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
		public String collection;

		@Option(name = "-query", metaVar = "[term]", required = true, usage = "query")
		public String query;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] argv) throws Exception {
		Args args = new Args();
		CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

		try {
			parser.parseArgument(argv);
		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
			return -1;
		}

		if (args.collection.endsWith(".gz")) {
			System.out.println("gzipped collection is not seekable: use compressed version!");
			return -1;
		}

		FileSystem fs = FileSystem.get(new Configuration());

		initialize(args.index, args.collection, fs);

		System.out.println("Query: " + args.query);
		long startTime = System.currentTimeMillis();
		runQuery(args.query);
		System.out.println("\nquery completed in " + (System.currentTimeMillis() - startTime) + "ms");

		return 1;
	}

	/**
	 * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new BooleanRetrievalJava(), args);
	}
}
