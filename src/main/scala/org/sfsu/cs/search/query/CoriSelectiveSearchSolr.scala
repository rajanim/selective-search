package org.sfsu.cs.search.query

import com.lucidworks.spark.util.SolrQuerySupport
import com.lucidworks.spark.util.SolrSupport._
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.response.QueryResponse

import scala.collection.mutable

/**
  * Created by rajanishivarajmaski1 on 5/14/18.
  */
class CoriSelectiveSearchSolr {

  var avgCw: Long = 0

  var arrayCw = new Array[Long](10)


  /**
    *
    * @param zkHost
    * @param clusterCollection
    * @param searchStatCollection
    */
  def init(zkHost: String, clusterCollection: String, searchStatCollection: String) = {
    val solrClient = getCachedCloudClient(zkHost)
    val solrQuery = new SolrQuery()
    solrQuery.set("collection", searchStatCollection)
    solrQuery.setQuery("id:cw")
    solrQuery.setFields("coriCwAvgCw_s")
    val resp = SolrQuerySupport.querySolr(solrClient, solrQuery, 0, null).get
    val doc = resp.getResults.get(0)
    val cw = doc.get("coriCwAvgCw_s").toString.trim.split(";")
    avgCw = cw(1).toLong
    val cwValues = cw(0).split(",")
    arrayCw = new Array[Long](cwValues.size)

    for (i <- 0 until cwValues.size) {
      arrayCw(i) = cwValues(i).toLong
    }
  }

  def executeCoriSelectiveSearch(zkHost: String, clusterCollection: String, coriStatsCollection: String,
                                 searchQuery: String, topShards: Int, b: Double, fields: String, rows: Int): (Int, QueryResponse) = {
    val shardScore = mutable.HashMap.empty[String, Double]

    val queryTerms = searchQuery.split(" ")
    var qTime = 0
    queryTerms.foreach(term => {
      val results = getMatchingShards(zkHost, clusterCollection, coriStatsCollection, term, "postingList_t")
      if(results!=null){
      qTime += results._1
        println("qtime  cori csi "+ results._1+ " totalQtime"+ qTime)
      val shards = results._2
      if (shards != null) {
        val cf = shards.filter(_ != 0).size
        var shardCnt = 0
        for (j <- 0 until shards.size) {
          shardCnt += 1
          val score = shardScore.getOrElse("shard" + shardCnt, 0.0)
          shardScore.put("shard" + shardCnt, getTIScore(shards(j), arrayCw(j), shards.size, cf, b) + score)
        }
      }
    }})


    val sortedMap = shardScore.toSeq.sortWith(_._2 > _._2)
    println(sortedMap.mkString("|"))
    if (!sortedMap.isEmpty) {
      val results = getMatchedDocs(clusterCollection, zkHost, searchQuery, sortedMap.take(topShards).toMap.keys.mkString(","), fields, rows)
      qTime += results.getQTime
      println("qtime after results obtained for cori query"+ results.getQTime + " total "+ qTime)
      (qTime, results)
    }
    else
      null
  }


  def getMatchingShards(zkHost: String, clusterCollection: String, coriStatsCollection: String,
                        searchQuery: String, fields: String): (Int, Array[Long]) = {

    //querySolr for postingList_t split by comma and return array of long
    val solrResponse = querySolr(zkHost, coriStatsCollection, searchQuery, fields)
    if (solrResponse.getResults.getNumFound > 0) {
      val shardsList = solrResponse.getResults.get(0).getFirstValue("postingList_t").toString
      (solrResponse.getQTime, getDfArray(shardsList))
    }
    else
      null
  }


  def querySolr(zkHost: String, coriStatCollection: String, searchQuery: String, fields: String):  QueryResponse = {
    val solrClient = getCachedCloudClient(zkHost)
    val solrQuery = new SolrQuery(searchQuery)
    solrQuery.set("collection", coriStatCollection)
    solrQuery.setFields(fields)

    SolrQuerySupport.querySolr(solrClient, solrQuery, 0, null).get


  }

  /**
    * each line in cori param file is term to document frequency of that term in the respective cluster by index.
    * this method parses line and created df array and returns it.
    *
    * @param string
    * @return
    */
  def getDfArray(string: String): Array[Long] = {
    val dfs = string.split(";")
    val array: Array[Long] = new Array[Long](dfs.size)

    for (i <- 0 until dfs.size) {
      array(i) = dfs(i).toInt
    }

    array
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
    val T = (df) / (df + 50 + (150 * (cw.toDouble / avgCw.toDouble)))
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
  def getMatchedDocs(clusterColl: String, zkHost: String, searchText: String, clusters: String, fields: String, rows: Int): QueryResponse = {
    val solrClient = getCachedCloudClient(zkHost)
    val solrQuery = new SolrQuery()
    solrQuery.set("collection", clusterColl)
    solrQuery.setQuery(searchText)
    solrQuery.set("_route_", clusters)
    solrQuery.set("fl", fields)
    solrQuery.setRows(rows)
    println("query to large collection", solrQuery.toQueryString)
    SolrQuerySupport.querySolr(solrClient, solrQuery, 0, null).get

  }
}

object CoriSelectiveSearchSolr {
  def main(args: Array[String]): Unit = {
    val coriSelectiveSearchSolr = new CoriSelectiveSearchSolr
    coriSelectiveSearchSolr.init("localhost:9983", "clueweb", "clueweb_cori")
    val response = coriSelectiveSearchSolr.executeCoriSelectiveSearch("localhost:9983", "clueweb", "clueweb_cori",
      "rest perfect", 10, 0.4, "id, score", 100)._2.getResults.iterator()
    print("\n\n\n")
    while (response.hasNext) {
      val doc = response.next()
      println(doc.get("id"), doc.get("score"))
    }

  }


}
