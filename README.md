# Selective Search


Selective search is a Distributed Information Retrieval(DIR) project for large-scale text search in which collection of documents are partitioned into subsets(known as shards) based on documents corpus similarity, thus enabling search across fewer shards instead of all shards.
 
 Goal of the project is to implement an efficient and effective distributed search system without affecting the search quality and aiming to reduce search cost. 
 
The methodology of decomposing a large-scale collection into subunits based on its records content similarity is termed as “Topic Based Partitioning”, and searching across only fewer relevant shards for given search query is termed as “Selective Search” in a [publication](online.sfsu.edu/ak/#publications) by Kulkarni et al. 



# General Architecture


![Overview1](https://raw.githubusercontent.com/rajanim/selective-search/master/docs/general_selective_search_arc.jpg)



# Current Implementation


## Technology Stack 
* Programming language : Scala
* Extended frameworks 
    * Apache Spark for Distributed Computing
        * Extends Apache Spark's Machine Learning Libraries(MLLib) for implementation of Topic Based Partitioning and algorithm is run on top of Spark Cluster. 
    * Apache Solr for Distributed Search
        * Extends Apache Solr's Information Retrieval(Search) libraries for implementation of various selective search algorithms and search cluster itself is setup on Apache SolrCloud. 
    * For integration between Apache Spark and Apache Solr, Spark-Solr libraries are utilized.


## Version Compatibility 
1. Java JDK 8 java version "1.8.0_131"

2. Scala 2.12.2

3. Apache Spark version 2.1.1

4. Apache solr 6.6.2

5. Spark-Solr 3.3.0

6. Apache Maven 3.5.0

7. Apache Ivy 2.4.0

8. Apache ANT version 1.10.1


## Implementation Architecture 
![Overview2](https://raw.githubusercontent.com/rajanim/selective-search/master/docs/impl_selective_search.jpg)


 
# Getting Started

### Compile
To compile the current version of selective-search, you will need the following list of software running on your machine.
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


## Examples
Follow the steps listed below to execute(run) selective search for any other(custom/specific)dataset.

* Create a scala class similar to NewsgroupRunner.scala
* Obtain spark context by creating a new `org.sfsu.cs.main.SparkInstance` and invoking `createSparkContext` method
`val sc = sparkInstance.createSparkContext("class name")`
* Transform input documents into RDD of TF Documents, example references as listed below
    * If input dataset is collection of text files, utilize the text parsers apis available under `org.sfsu.cs.io.text.TextFileParser.scala` to obtain text docs mapped to `org.sfsu.cs.document.TFDocument`
    * if input dataset is collection of clueweb records, utilize the parser available under `org.sfsy.cs.io.clueweb09`
    * If input dataset is csv file, utilize the parser apis available under `org.sfsu.cs.io.csv`


## Configuration and Tuning
Current configuration


## Troubleshooting tips and suggestions
Memory settings

Spark tuning


## References
