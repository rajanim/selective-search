# Selective Search


Selective search is a Distributed Information Retrieval(DIR) project for large-scale text search in which collection of documents are partitioned into subsets(known as shards) based on documents corpus similarity, thus enabling search across fewer shards instead of all shards.
 
 Goal of the project is to implement an efficient and effective distributed search system without affecting the search quality and aiming to reduce search cost. 
 
The methodology of decomposing a large-scale collection into subunits based on its records content similarity is termed as “Topic Based Partitioning”, and searching across only fewer relevant shards for given search query is termed as “Selective Search” in a [publication](http://online.sfsu.edu/ak/#publications) by Kulkarni et al. 


# Table of Contents
1. [General Architecture](https://github.com/rajanim/selective-search#general-architecture)
2. [Current Implementation](https://github.com/rajanim/selective-search#current-implementation)
    1. [Technology Stack](https://github.com/rajanim/selective-search#technology-stack)
    2. [Version Compatibility](https://github.com/rajanim/selective-search#version-compatibility)
    3. [Implementation Architecture](https://github.com/rajanim/selective-search#implementation-architecture)
    4. [Features](https://github.com/rajanim/selective-search#features)
3. [Getting Started](https://github.com/rajanim/selective-search#getting-started)
    1. [Compile](https://github.com/rajanim/selective-search#compile)
    2. [Run](https://github.com/rajanim/selective-search#run)
        1. [Run on IDE](https://github.com/rajanim/selective-search#1-run-on-ide)
        2. [Run on Spark Cluster](https://github.com/rajanim/selective-search#2-run-on-spark-cluster)
    3. [Setup Solrcloud](https://github.com/rajanim/selective-search#setup-solrcloud)
4. [Examples](https://github.com/rajanim/selective-search#examples)
5. [Configuration and Tuning](https://github.com/rajanim/selective-search#configuration-and-tuning)
6. [Questions](https://github.com/rajanim/selective-search#questions)
7. [References](https://github.com/rajanim/selective-search#references)



# General Architecture


![Overview1](https://raw.githubusercontent.com/rajanim/selective-search/master/docs/general_selective_search_arc.jpg)



# Current Implementation
Selective Search is programmed in Scala. It extends libraries of Apache Spark MLLib for unsupervised clustering and distributed computing, Apache Solr libraries for Search and Information Retrieval.

## Technology Stack 
* Programming language : Scala
* Extended frameworks 
    * Apache Spark for Distributed Computing
        * Extends Apache Spark's Machine Learning Libraries(MLLib) for implementation of Topic Based Partitioning and algorithm is run on top of Spark Cluster. 
    * Apache Solr for Distributed Search
        * Extends Apache Solr's Information Retrieval(Search) libraries for implementation of various selective search algorithms and search cluster itself is setup on Apache SolrCloud. 
    * For integration between Apache Spark and Apache Solr, Spark-Solr libraries are utilized.
* Tools
    * Maven for build, IntelliJ for development, Git for SCM

## Version Compatibility 
1. Java JDK 8 version "1.8.0_131"

2. Scala 2.12.2

3. Apache Spark version 2.1.1

4. Apache solr 6.6.2

5. Spark-Solr 3.3.0

6. Apache Maven 3.5.0

7. Apache Ivy 2.4.0

8. Apache ANT version 1.10.1


## Implementation Architecture 
![Overview2](https://raw.githubusercontent.com/rajanim/selective-search/master/docs/impl_selective_search.jpg)

# Features
* Converting textual data to TF-IDF vectors either based on dictionary of terms supplied as input or otherwise dictionary is generated from training dataset by applying TF-IDF weights to terms.
* Custom KMeans algorithm implementation for clustering textual dataset, extending Apache Spark MLLib libraries, implement both similarity computation measures, namely, Cosine and Euclidean.
* Index records into Solrcloud.
* Selective search algorithms implementation, namely, CORI[6] and ReDDE[7]
* Parsers for Clueweb09 WARC Records, 20newsgroup dataset, and in general, text dataset.
* Text analyzers that strips html, extracts texts content, tokenize, filter by stopwords and apply KStemmer. 

 
# Getting Started

### Compile
To compile the current version of selective-search, you will need to have the following list of software running on your machine.
* Java, Scala, Apache Maven (versions as listed in version compatibility section)
 
 In order to verify the above listed softwares are running on your machine, confirm with commands below.
 * `java -version` 
    *  java version "1.8.0_31"
 * `scala -version`
    * Scala code runner version 2.11.8
 * `mvn -version`
    * Apache Maven 3.3.9


After verification of required softwares setup, download the source code and execute the below command.
* `mvn clean install -DskipTests`


### Run
To run the selective-search project on localhost(machine), it is required for Apache SolrCloud to be configured. If you do not have it already configured, follow instructions provided here: ()

After the SolrCloud setup, there are two ways to run selective-search, either set it up on an IDE—it could be either IntelliJ/Eclipse or launch a Spark Cluster and execute job on it.


#### 1. Run on IDE

* Download or clone the project
`git clone https://github.com/rajanim/selective-search.git`
* Compile source code by following steps listed under "Compile" section
* Import the current project as Maven project into the IDE
* To test this setup, go to the test app `App.scala`, right click and run. It should print 'Hello World!' This confirms scala code is compiled and running successfully.     
* To test selective search setup, go to `TestTextFileParser.scala`, modify the root path as per your directory settings, run the test cases(right click and choose run option)
* To execute 20newsgroup selective search, navigate to `org.sfsu.cs.io.newsgroup.NewsgroupRunner.scala`, modify input directory path to your local machine directory path, run.
* Navigate to /test/org.sfsu.cs.* to test various functionality implemented.     

#### 2. Run on Spark Cluster

Configure Spark Cluster on localhost.

* Download Apache Spark (current implementation runs on spark `spark-2.0.0-bin-hadoop2.7`)
* Go to terminal and navigate to the path where spark unzip is located. For example, `cd /Users/user_name/softwares/spark-2.0.0-bin-hadoop2.7`
* Configure spark
    * start spark master `./sbin/start-master.sh `
    * configure spark slave instances
        * create spark-env.sh file using the provided template:
    	`cp ./conf/spark-env.sh.template ./conf/spark-env.sh`
    
    	* append a configuration param to the end of the file:
    	`echo "export SPARK_WORKER_INSTANCES=4" >> ./conf/spark-env.sh`
   * start spark slaves instances
    `./sbin/start-slaves.sh <master_url> [master url: available or seen on spark web-ui after master is started, at: localhost:8080]`

Run the selective search project on spark cluster.
`nohup ./bin/spark-submit --master spark://RajaniM-1159:7077 --num-executors 2  --executor-memory 8g  --driver-memory 12g --conf "spark.rpc.message.maxSize=2000" --conf "spark.driver.maxResultSize=2000"  --class com.sfsu.cs.main.TopicalShardsCreator  /Users/rajanishivarajmaski/selective-search/target/selective-search-1.0-SNAPSHOT.jar TopicalShardsCreator  -zkHost localhost:9983 -collection word-count -warcFilesPath /Users/rajani.maski/rm/cluweb_catb_part/ -dictionaryLocation /Users/rajani.maski/rm/spark-solr-899/ -numFeatures 25000 -numClusters 50 -numPartitions 50 -numIterations 30  &`


### Setup SolrCloud
* Download Apache Solr as zip(current implementation runs on solr 6.2.1)
* Got to terminal and navigate to path where solr unzip directory is located.
* Start solr in cloud mode

	`./bin/solr start -c -m 4g`
* Once you have solr started with welcome message as 'Happy Searching!', you should be able to connect to its admin UI
`http://localhost:8983/solr/#/`, navigate to collections: `http://localhost:8983/solr/#/~collections`
* Add a collection by giving a collection name, choose config set, it should be data_driven_schema_configs.  Other key value pair inputs as seen here in screenshot : [click here](https://raw.githubusercontent.com/rajanim/selective-search/master/docs/misc/solrcloud_implicit.png) 
* Confirm if the collection is created succesfully by navigating to cloud admin ui `http://localhost:8983/solr/#/~cloud` and you should be able to see newly created collection
* Another approach to create a collection is via command line. Example command `./bin/solr create_collection -n data_driven_schema_configs -c collection_name -p portNumber -shards numShards`

For `Selective Search`, we require a solr collection with `implicit routing` strategy. 
* Example curl command to create a collection of 20 shards with implicit routing
`http://localhost:8983/solr/admin/collections?action=CREATE&name=NewsGroupImplicit&numShards=20&replicationFactor=1&maxShardsPerNode=20&router.name=implicit&shards=shard1,shard2,shard3,shard4,shard5,shard6,shard7,shard8,shard9,shard10,shard11,shard12,shard13,shard14,shard15,shard16,shard17,shard18,shard19,shard20&collection.configName=data_driven_schema_configs&router.field=_route_`

### Visualization
* Cluster allocation visualization by utilizing [Banana](https://github.com/lucidworks/banana) dashboard.
* Tutorial and reference notes [here](https://doc.lucidworks.com/lucidworks-hdpsearch/2.5/Guide-Banana.html)
* Configure Banana on your solr instance and connect to banana dashboard via http url 
`http://localhost:8983/solr/banana/src/index.html#/dashboard`


## Examples
Follow the steps listed below to execute(run) selective search for any other(custom/specific)dataset.

* Create a scala class similar to NewsgroupRunner.scala
* Obtain spark context by creating a new `org.sfsu.cs.main.SparkInstance` and invoking `createSparkContext` method
`val sc = sparkInstance.createSparkContext("class name")`
* Transform input documents into RDD of TF Documents, example references as listed below
    * If input dataset is collection of text files, utilize the text parsers apis available under `org.sfsu.cs.io.text.TextFileParser.scala` to obtain text docs mapped to `org.sfsu.cs.document.TFDocument`
    * if input dataset is collection of clueweb records, utilize the parser available under `org.sfsu.cs.io.clueweb09`
    * If input dataset is csv file, utilize the parser apis available under `org.sfsu.cs.io.csv`


## Configuration and Tuning
Required configurations
* All the softwares required to compile source, as listed under [Compile]() section are required to be installed
* Solrcloud need to be configured, either on local machine or remote, if setup on remote, make sure to specify correct ip and port while executing the job. Config steps [here]()
* Spark stand alone cluster setup on local machine to run job via spark-submit.sh

Tuning
* Spark parameters setting --executor-memory, -driver-memory based on memory available on machine.
* Spark driver may report maxResultSize exception, it can be increased by passing param `"spark.driver.maxResultSize=2000"` it cannote exceed 2048
* For Apache solr, allocate Xmx while starting solr. For example, to allocate 4g, `./bin/solr start -c -m 4g`

## Troubleshooting

* Received an exception while creating a solr collection that reported "shards requested are higher than the allowed number" `https://issues.apache.org/jira/browse/SOLR-4114`
Fix : Make sure `maxShardsPerNode` is equal to `numShards` based on number of nodes available.
## Questions
If you have additional questions related to implementation, experimental results, benchmarks or usage of   `Selective-Search`, happy to help. Please email to rajani.maski@gmail.com

## References
1. Anagha Kulkarni. 2015. Selective Search: Efficient and Effective Large­ scale Search. ACM Transactions on Information Systems, 33(4). ACM. 2015.
2.  Anagha Kulkarni. 2010. Topic-based Index Partitions for Efficient and Effective Selective Search. 8th Workshop on Large-Scale Distributed Systems for Information Retrieval.
3.  Rolf Jagerman, Carsten Eickhoff. Web-scale Topic Models in Spark: An Asynchronous Parameter Server. 2016.
4.  Clueweb09 dataset. Lemur Project.
5.  20Newsgroups. Jrennie. qwone.com/~jason/20Newsgroups
6.  Mon Shih Chuang and Anagha Kulkarni. Improving Shard Selection for Selective Search.  Proceedings of the Asia Information Retrieval Societies Conference. November 2017
7.  L. Si and J. Callan. (2003.) "Relevant document distribution estimation method for resource selection." In Proceedings of the Twenty Sixth Annual International ACM SIGIR Conference on Research and Development in Information Retrieval. Toronto: ACM.