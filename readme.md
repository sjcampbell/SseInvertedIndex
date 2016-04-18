# Building a Secure, Searchable, Encrypted Index in Spark #  


#### For project details, see the project paper [here](Project%20Paper/Building%20a%20Secure%20Searchable%20Encryption%20Index%20in%20Apache%20Spark.pdf)  


#### Repository Notes:  
* There are two encrypted index generating algorithms. The master branch contains PI<sub>pack</sub>, the postings grouping algorithm. The 'pair-postings' branch contains PI<sub>bas</sub>, the postigs pairs algorithm.
* There are query execution classes. The scala version does not work with HDFS files yet, so just use the java version, which is run from the 'query.sh', as described below.

#### To run it:
1. Clone the repo (master branch): sjcampbell/bigdata
2. Build the source: mvn package
3. Build an encrypted index: Run buildIndex.sh with arguments: <input data file> <output index file> <group size>
  3. For a quick test run, use the Shakespeare dataset.  
    ``mkdir data``  
    ``curl http://lintool.github.io/bespin-data/Shakespeare.txt > data/Shakespeare.txt``  
    ``./buildIndex.sh data/Shakespeare.txt index-Shakespeare-GroupBy12 12``  
  3. For a larger test, run on Altiscale using the 10% English Wikipedia dataset  
    ``./buildIndex.sh /shared/cs489/data/enwiki-20151201-pages-articles-0.1sample.txt index-WikiEn0.1-GroupBy12 12``
4. Execute a search: Run query.sh with arguments: <index path> <dataset path> <query word>
  4. Shakespeare example:
    ``./query.sh indexGroupBy12 data/Shakespeare.txt prognostication``
  4. Wiki example:
    ``./query.sh index-Wikien0.1-GroupBy12 /shared/cs489/data/enwiki-20151201-pages-articles-0.1sample.txt prognostication``
