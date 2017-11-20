# Selective Search


Selective search is a Distributed Information Retrieval(DIR) project for large-scale text search in which collection of documents are partitioned into subsets(known as shards) based on documents corpus similarity, thus enabling search across fewer shards instead of all shards.
 
 Goal of the project is to implement an efficient and effective distributed search system without affecting the search quality and aiming to reduce search cost. 
 
The methodology of decomposing a large-scale collection into subunits based on its records content similarity is termed as “Topic Based Partitioning”, and searching across only fewer relevant shards for given search query is termed as “Selective Search” in a [publication](online.sfsu.edu/ak/#publications) by Kulkarni et al. 



# General Architecture


![Overview1](https://raw.githubusercontent.com/rajanim/selective-search/master/docs/general_selective_search_arc.jpg)

#

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


 
# Getting started

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
To run the selective-search project on your localhost, you would initially require Apache SolrCloud configured, to setup Apache Solr, follow instructions provided here: ()
Post that setup, you have three ways to run selective-search
    1) Run as a jar file, this will spin spark locally and shuts down after program completion.
    2) Run on IntelliJ/Eclipse IDE.
    3) Launching a Spark Cluster and running on it.


## Examples
To execute selective search for custom(any other) dataset by extending the current project, follow the below steps



## Configuration and Tuning
Current configuration


## Troubleshooting tips and suggestions
Memory settings

Spark tuning


## References
