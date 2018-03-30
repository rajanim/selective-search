package search.query

import com.lucidworks.spark.util.SolrQuerySupport
import com.lucidworks.spark.util.SolrSupport._
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.common.SolrDocumentList
import search.helper.CORIHelper

import scala.collection.mutable

/**
  * Created by rajanishivarajmaski1 on 11/17/17.
  */
class CORISelectiveSearch {

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
                                 searchQuery: String, b: Double): SolrDocumentList = {
    val shardScore = mutable.HashMap.empty[String, Double]

    val shards = coriHelper.termDfMap.get(searchQuery).get
    val cf = shards.filter(_ != 0).size
    var shardCnt=0
    for (j <- 0 until shards.size ) {
      shardCnt+=1
      shardScore.put("shard"+shardCnt, getTIScore(shards(j), coriHelper.arrayCw(j), shards.size, cf, b))
    }

    val sortedMap = shardScore.toSeq.sortWith(_._2 > _._2)
    println(sortedMap.mkString("|"))
    getMatchedDocs(clusterCollection, zkHost, searchQuery, sortedMap.take(sortedMap.size).toMap.keys.mkString(","))
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
  def getMatchedDocs(clusterColl: String, zkHost: String, searchText: String, clusters: String): SolrDocumentList = {
    val solrClient = getCachedCloudClient(zkHost)
    val solrQuery = new SolrQuery()
    solrQuery.set("collection", clusterColl)
    solrQuery.setQuery(searchText)
    solrQuery.set("_route_", clusters)
    solrQuery.set("fl", "clusterId_s", "score", "content_t", "id")
    solrQuery.setRows(10)
    println("query to large collection", solrQuery.toQueryString)
    val collResp = SolrQuerySupport.querySolr(solrClient, solrQuery, 0, null)
    collResp.get.getResults
  }


}
