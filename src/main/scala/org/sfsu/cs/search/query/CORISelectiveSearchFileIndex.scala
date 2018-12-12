package org.sfsu.cs.search.query

import com.lucidworks.spark.util.SolrQuerySupport
import com.lucidworks.spark.util.SolrSupport._
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.common.SolrDocumentList
import org.sfsu.cs.search.helper.CORIHelper

import scala.collection.mutable

/**
  * Created by rajanishivarajmaski1 on 11/17/17.
  */
class CORISelectiveSearchFileIndex {

  var coriHelper: CORIHelper = null


  def getCoriHelper = coriHelper

  /**
    *Initialize Cori Selective Search by loading CORI related required params into memory.
    * @param file
    */
  def initCoriSelectiveSearch(file: String): Unit = {
    coriHelper = new CORIHelper
    coriHelper.readCORIParamsFromFile(file)
  }

  /**
    * Compute CORI score and select shards
    *
    * @param zkHost
    * @param clusterCollection
    * @param searchQuery
    * @param b
    */
  def executeCoriSelectiveSearch(zkHost: String, clusterCollection: String,
                                 searchQuery: String, topShards: Int, b: Double, fields:String): SolrDocumentList = {
    val shardScore = mutable.HashMap.empty[String, Double]
    val queryTerms = searchQuery.split(" ")
    queryTerms.foreach(term => {
      if(coriHelper.termDfMap.contains(term.trim)){

      val shards = coriHelper.termDfMap.get(term.trim).get
      val cf = shards.filter(_ != 0).size
      var shardCnt=0
      for (j <- 0 until shards.size ) {
        shardCnt+=1
        val score = shardScore.getOrElse("shard" + shardCnt, 0.0)
        val finalScore = getTIScore(shards(j), coriHelper.arrayCw(j), shards.size, cf, b) + score
        shardScore.put("shard" + shardCnt, finalScore)
      }

    }})


    val sortedMap = shardScore.toSeq.sortWith(_._2 > _._2)
    println(sortedMap.mkString("|"))
    getMatchedDocs(clusterCollection, zkHost, searchQuery, sortedMap.take(topShards).toMap.keys.mkString(","), fields)
  }


  /**
    * To compute CORI score
    *
    * @param df
    * @param cw
    * @param numShards
    * @param cf
    * @param b
    * @return
    */
  def getTIScore(df: Long, cw: Long, numShards: Int, cf: Int, b: Double): Double = {
    val T = (df) / (df + 50 + (150 * (cw.toDouble / coriHelper.avgCw.toDouble)))
    val I = Math.log10((numShards + 0.5) / cf.toDouble) / Math.log10(numShards + 1.0)
    b + (1 - b) * T * I
  }

  /**
    * get matched docs by querying only relevant shards selected by applying CORI.
    *
    * @param clusterColl
    * @param zkHost
    * @param searchText
    * @param clusters
    * @return
    */
  def getMatchedDocs(clusterColl: String, zkHost: String, searchText: String, clusters: String, fields: String): SolrDocumentList = {
    val solrClient = getCachedCloudClient(zkHost)
    val solrQuery = new SolrQuery()
    solrQuery.set("collection", clusterColl)
    solrQuery.setQuery(searchText)
    solrQuery.set("_route_", clusters)
    solrQuery.set("fl", fields)
    solrQuery.setRows(10)
    println("query to large collection", solrQuery.toQueryString)
    val collResp = SolrQuerySupport.querySolr(solrClient, solrQuery, 0, null)
    collResp.get.getResults
  }


}
