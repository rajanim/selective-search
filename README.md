# Selective Search


Selective search is a Distributed Information Retrieval(DIR) project for large-scale text search in which collection of documents are partitioned into subsets(known as shards) based on documents corpus similarity, thus enabling search across fewer shards instead of all shards.
 
 Goal of the project is to implement an efficient and effective distributed search system without affecting the search quality and aiming to reduce search cost. 
 
The methodology of decomposing a large-scale collection into subunits based on its records content similarity is termed as “Topic Based Partitioning”, and searching across only fewer relevant shards for given search query is termed as “Selective Search” in a [publication](online.sfsu.edu/ak/#publications) by Kulkarni et al. 


## Architecture Overview

![Overview](https://github.com/rajanim/selective-search/blob/master/docs/selective_search.jpg)
![Overview1](selective-search/docs/selective_search.jpg)


## Technology Stack 
Programming language : Scala
Extended frameworks : Apache Spark for distributed computing, Apache Solr for distributed search. 

Extends Apache Spark's Machine Learning Libraries(MLLib) for implementation of Topic Based Partitioning and runs on top of Spark Cluster. 
Extends Apache Solr Information Retrieval(Search) libraries for implementation of various selective search algorithms and search cluster itself is setup on Apache SolrCloud. For integration between Apache Spark and Apache Solr, Spark-Solr libraries are utilized.


## Version Compatibility 
1. Java JDK 8 java version "1.8.0_131"

2. Scala 2.12.2

3. Apache Spark version 2.1.1

4. Apache solr 6.6.2

5. Spark-Solr 3.3.0

6. Apache Maven 3.5.0

7. Apache Ivy 2.4.0  :: http://ant.apache.org/ivy

8. Apache ANT version 1.10.1

9. code from git repo :: https://github.com/rajanim/selective-search.git


 
## Getting started
### Compile
To compile the current version of selective-search, you will need Apache Maven which will take care of importing all the libraries dependencies. After downloading the project, you can compile it from the repository directory by executing following commands
        mvn clean install -DskipTests
     

### Features


## Examples

## Configuration and Tuning

## Troubleshooting tips and suggestions

## Developing a custom parser

## References
