# Building a Secure, Searchable, Encrypted Index in Spark


#### For project details, see the project paper [here](Project%20Paper/Building%20a%20Secure%20Searchable%20Encryption%20Index%20in%20Apache%20Spark.pdf)  


#### Repository Notes:  
* There are two encrypted index generating algorithms. The master branch contains PI<sub>pack</sub>, the postings grouping algorithm. The 'pair-postings' branch contains PI<sub>bas</sub>, the postings pairs algorithm. PI<sub>bas</sub> should probably only be run on toy datasets.
* There are query execution classes. The scala version does not work with HDFS files yet, so just use the java version, which is run from the 'query.sh', as described below.

#### To run it:
1. Clone the repo (master branch): sjcampbell/bigdata
2. Build the source: ``mvn package``
3. Build an encrypted index: Run buildIndex.sh with arguments (chmod +x buildIndex.sh):  
  ``input data file``  
  ``output index file``  
  ``group size``
  3. For a quick test run, use the Shakespeare dataset.  
    ``./buildIndex.sh /shared/cs489/data/Shakespeare.txt index-Shakespeare-GroupBy12 12``  
  3. For a larger test, run on Altiscale using the 10% English Wikipedia dataset  
    ``./buildIndex.sh /shared/cs489/data/enwiki-20151201-pages-articles-0.1sample.txt index-WikiEn0.1-GroupBy12 12``  
    or manually,  
    ``my-spark-submit --driver-memory 6g \\  ``  
      ``--class com.sjcampbell.spark.sse.BuildInvertedIndex target/bigdata-0.1.0-SNAPSHOT.jar \\ ``  
      ``--input /shared/cs489/data/enwiki-20151201-pages-articles-0.1sample.txt  \\ ``  
      ``--output index-WikiEn0.1-GroupBy12 --doc-group-size 12 ``  

4. Execute a search: Run query.sh with arguments:  
  ``index path``  
  ``dataset path``  
  ``query word``  
  4. Shakespeare example:
    ``./query.sh index-Shakespeare-GroupBy12 /shared/cs489/data/Shakespeare.txt prognostication``
  4. Wiki example:
    ``./query.sh index-Wikien0.1-GroupBy12 /shared/cs489/data/enwiki-20151201-pages-articles-0.1sample.txt prognostication``  
  
#### Troubleshooting:
I had to increase driver and executor memory when running spark-submit with the PI<sub>pack</sub> implementation (master branch). And even then, occasionally a node would fail, but the job would still complete.  
``--driver-memory 10G --executor-memory 8G``
