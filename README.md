# Selective Search

Selective search is a Distributed Information Retrieval(DIR) project that implements an effective and efficient distributed search system by following an approach of dividing large scale collection into subunits and to perform search across fewer shards instead of all shards without affecting the search quality and aiming to reduce search cost. 
The methodology of dividing huge collection into subsets based on its documents similarity is termed as “Topic Based Partitioning” and searching across only fewer relevant shards for given search query is termed as “Selective Search” in a [publication](online.sfsu.edu/ak/#publications) by Kulkarni et al. 

# Introduction
Distributed Information Retrieval(DIR) system, that is designed to search for data spread across different machines, has been the key architecture of the modern search engine frameworks. These frameworks follow a peer-to-peer DIR system architecture—in which each computing resource is outlined to consist of a part of dataset with local index built on top of it. Further, each resource is presumed to handle a given search query independently. The need of DIR is to handle massive datasets that cannot fit into a single machine. We have been witnessing modern search based applications that are built on such DIR notion have been handling incredible volume of search queries that are in order of few billions per day. Although this architecture scales for larger datasets, it has issues with higher computation cost required to search exhaustively across each partition(shard) of a collection.

As a part of this project work, we aim to investigate on partitioning massive collection into subunits such that similar documents that may satisfy user intent are grouped together, only fewer shards are searched for a given search query. Such a DIR technique has been termed as selective search[2, section 3.3]. Selective search is based on the cluster hypothesis that states documents with similar content are likely to match given same query, and hence, for any search request, only fewer appropriate shards consisting of these similar records must be searched. The procedure followed for selective search is, grouping of similar documents into topic shards at the time of index creation, and at query time, shard ranking algorithm[1][3] is applied to select few shards that are likely to contain relevant documents. The results from each selected shard are merged to obtain search results. Research work by Kulkarni et al. [3] communicate that selective search reduces required computation cost at query time and still maintain the quality of search  results[3]. 

Currently, experimentation carried out is with respect to partitioning of collections as topic shards by applying variant k-mean clustering techniques. One of the main challenges tackled as a part of this work is grouping homogeneous documents into its respective shards. Standard techniques of clustering data either trade off cluster quality over efficiency or vice versa. Simulation and experimental analysis conducted with two types of datasets, namely, 20 newsgroup[5] and apportion of Category B of ClueWeb09 (CW09-B)[4], reveal the desired results for the suggested approaches on selective search tasks. There are certain obvious improvements on search results effectiveness and stability over the standard exhaustive search results. The selective search results appear effective at Recall. Clustering approaches examined produce a balance sizes shards which is serviceable for load balancing and managing query time. 	
 
# Implementation 
Selective search extends and utilizes machine learning libraries of Apache Spark(MLLib) to implement clustering phase of selective search. Clustering is executed on top of Apache Spark Cluster.
For search phase, Apache SolrCloud is setup that consist of topic shards(as generated during clustering phase) and various selective search algorithms are implemented on top of Apache Solr.
Spark-Solr libraries are used for connectivity between Apache Spark and Apache Solr frameworks.

# Version Compatibility 


# Usage
 
## Getting started

## Features

## Examples

## Configuration and Tuning

## Troubleshooting tips and suggestions

## Developing a custom parser


