package org.sfsu.cs.search.query

import com.lucidworks.spark.util.SolrQuerySupport
import com.lucidworks.spark.util.SolrSupport._
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.common.SolrDocumentList

import scala.collection.mutable

/**
  * Created by rajanishivarajmaski1 on 11/17/17.
  */
class ReDDESelecitveSearch {

  /**
    *
    * @param zkHost
    * @param csIndexColl
    * @param clusterColl
    * @param searchText
    * @param numRowsCSIndex
    * @param numShards
    * @return
    */
  def relevantDDEBasedSelectiveSearch(zkHost: String, csIndexColl: String, clusterColl: String, searchText: String,
                                      numRowsCSIndex: Int, numShards: Int, rowsToRet:Int): (Int, SolrDocumentList) = {
    //For the org.sfsu.cs.search term/text build a org.sfsu.cs.search query to cs index, get first 100 docs
    var totalQtime = 0
    var solrQuery = new SolrQuery()
    solrQuery.set("collection", csIndexColl)
    solrQuery.setRows(numRowsCSIndex)
    solrQuery.setQuery(searchText)
    solrQuery.set("fl", "clusterId_s", "score", "_route_")
    val solrClient = getCachedCloudClient(zkHost)
    println(solrQuery.toQueryString)
    val resp = SolrQuerySupport.querySolr(solrClient, solrQuery, 0, null).get
    totalQtime += resp.getQTime
    val results = resp.getResults
    println("numFound: ", results.getNumFound, "Rows retrieved: ", results.size)
    //for results, create map of cluster id vs score of matched docs, and select shard with max score
    val clusters = getRelevantClusters(results, numShards)
    if(!clusters.isEmpty) {

      // for org.sfsu.cs.search text/term, build a org.sfsu.cs.search query to cluster coll to query only retrieved shards.
      solrQuery = new SolrQuery()
      solrQuery.set("collection", clusterColl)
      solrQuery.setQuery(searchText)
      solrQuery.set("_route_", clusters)
      solrQuery.set("fl", "clusterId_s", "score", "content_t", "id")
      solrQuery.setRows(rowsToRet)
      println("query to large collection", solrQuery.toQueryString)
      val collResp = SolrQuerySupport.querySolr(solrClient, solrQuery, 0, null).get
      totalQtime += collResp.getQTime
      (totalQtime, collResp.getResults)
    } else
      null

  }


  /**
    *
    * @param results
    * @param numShards
    * @return
    */
  def getRelevantClusters(results: SolrDocumentList, numShards: Int): String = {
    val cidScore = mutable.HashMap.empty[String, Double]
    for (j <- 0 until results.size()) {
      val solrDoc = results.get(j)
      val cid = solrDoc.get("_route_").toString.replace("[", "").replace("]", "").trim
      val score = solrDoc.get("score").toString.toDouble
      val updatedScore = cidScore.getOrElseUpdate(cid, 0.0)
      cidScore.put(cid, updatedScore+score)
    }
    val sortedMap = cidScore.toSeq.sortWith(_._2 > _._2)
    println(sortedMap.mkString("|"))

    if(sortedMap.size>numShards)
      sortedMap.take(numShards).toMap.keys.mkString(",")
    else
      sortedMap.take(sortedMap.size).toMap.keys.mkString(",")

  }

}
