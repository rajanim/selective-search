package search.helper

import com.lucidworks.spark.util.{SolrQuerySupport, SolrSupport}
import com.lucidworks.spark.util.SolrSupport._
import org.apache.solr.client.solrj.SolrQuery
import org.sfsu.cs.utils.Utility

import scala.collection.mutable

/**
  * Created by rajanishivarajmaski1 on 11/17/17.
  */
class CORIHelper {

  var avgCw: Long = 0
  val termDfMap = mutable.HashMap.empty[String, Array[Long]]

  var arrayCw = new Array[Long](10)

  def getTermDFMAp() = termDfMap
  def getArrayCw() = arrayCw
  def getAvgCw = avgCw
  /**
    * Log term to document frequency of each term to file with cw and avg cw parameters.
    * @param clusterCollection
    * @param zkHost
    * @param numClusters
    */
  def logTermDFIndexCwAvgCwForCORI(clusterCollection: String, zkHost: String, numClusters: Int): Unit = {
    val sb = new StringBuilder
    sb.append(getCoriCwAvgCwParams(zkHost, clusterCollection)).append(getTermDFIndexForCori(clusterCollection, zkHost, 2))
    Utility.writeToFile(sb.toString(), Utility.getFilePath())
  }

  /**
    * To build and get term to document frequency of term in each cluster.
    * @param clusterCollection
    * @param zkHost
    * @param numClusters
    * @return
    */
  def getTermDFIndexForCori(clusterCollection: String, zkHost: String, numClusters: Int): String = {
    //http://localhost:8983/solr/Test/select?indent=on&q=*:*&wt=json&terms.limit=-1
    // &terms.fl=content_t&shards.qt=terms&rows=0&omitHeader=true&terms=true
    val solrClient = getCachedCloudClient(zkHost)
    val solrQuery = new SolrQuery("*:*")
    solrQuery.set("collection", clusterCollection)
    solrQuery.setRows(0)
    solrQuery.setRequestHandler("terms")
    solrQuery.setTerms(true)
    solrQuery.setTermsLimit(-1)
    solrQuery.addTermsField("content_t")
    val resp = SolrQuerySupport.querySolr(solrClient, solrQuery, 0, null)
    val results = resp.get.getTermsResponse
    val termsFreq = results.getTerms("content_t")
    val iter = termsFreq.iterator()
    val sb = new StringBuilder
    while (iter.hasNext) {
      //http://localhost:8983/solr/Test/select?indent=on
      // &q=term&wt=json&facet.limit=-1&facet.field=clusterId_i&rows=0&omitHeader=true&facet=true
      val item = iter.next()
      val solrSubQuery = new SolrQuery(item.getTerm)
      solrSubQuery.set("collection", clusterCollection)
      solrSubQuery.setRows(0)
      solrSubQuery.setFacet(true)
      solrSubQuery.setFacetLimit(-1)
      solrSubQuery.addFacetField("clusterId_i")
      solrSubQuery.set("omitHeader", "true")
      val subResp = SolrQuerySupport.querySolr(solrClient, solrSubQuery, 0, null)
      val clusterResults = subResp.get.getFacetField("clusterId_i")
      val facetIter = clusterResults.getValues.iterator
      sb.append(item.getTerm).append("|")
      while (facetIter.hasNext) {
        val cluster = facetIter.next
        sb.append(cluster.getCount).append(";")
      }
      sb.deleteCharAt(sb.lastIndexOf(";"))
      sb.append("\n")

    }
    sb.toString()

  }

  /**
    *Cw is total term frequency(ttf) of the cluster, array of Cw is ttf of each cluster in large clustered collection.
    * @param zkHost
    * @param clusterCollection
    * @return
    */
  def getCoriCwAvgCwParams(zkHost: String, clusterCollection: String): String = {
    val shardList = SolrSupport.buildShardList(zkHost, clusterCollection)
    val array = new Array[Long](shardList.size)
    var count = 0
    shardList.iterator.foreach(
      shard => {
        val url = shard.replicas.head.replicaUrl
        val solrClient = SolrSupport.getNewHttpSolrClient(url, zkHost)
        val solrQuery = new SolrQuery("*:*")
        solrQuery.setRequestHandler("terms")
        solrQuery.setRows(0)
        solrQuery.setTerms(true)
        solrQuery.setTermsLimit(-1)
        solrQuery.addTermsField("content_t")
        solrQuery.setDistrib(false)
        solrQuery.setParam("shards", url)
        solrQuery.setParam("shards.qt", "terms")
        solrQuery.setParam("distrib", false)
        val resp = SolrQuerySupport.querySolr(solrClient, solrQuery, 0, null)
        val results = resp.get.getTermsResponse
        val termsFreq = results.getTerms("content_t")
        val iter = termsFreq.iterator()
        var sum: Long = 0
        while (iter.hasNext) {
          val item = iter.next()
          sum += item.getFrequency
        }
        array(count) = sum
        count += 1
      }
    )
    val sb = new StringBuilder
    sb.append(array.mkString(",")).append(";").append((array.sum / shardList.size).toInt).append("\n")
    sb.toString()
  }


  /**
    * Load the CORI params into memory
    * @param file
    * @param n
    */
  def readCORIParamsFromFile(file: String, n: Int = 10000000): Unit = {
    val fileLinesIter = scala.io.Source.fromFile(file).getLines()
    val cw = fileLinesIter.next().split(";")
    avgCw = cw(1).toLong
    val cwValues = cw(0).split(",")
    arrayCw = new Array[Long](cwValues.size)

    for (i <- 0 until cwValues.size) {
      arrayCw(i) = cwValues(i).toLong
    }
    //val termDfMap = mutable.HashMap.empty[String, Array[Long]]
    while (fileLinesIter.hasNext) {
      val termDF = fileLinesIter.next()
      val term = termDF.split("\\|")
      val df = getDfArray(term(1))
      termDfMap.put(term(0), df)
    }
  }

  /**
    * each line in cori param file is term to document frequency of that term in the respective cluster by index.
    * this method parses line and created df array and returns it.
     * @param string
    * @return
    */
  def getDfArray(string: String): Array[Long] = {
    val dfs = string.split(";")
    val array: Array[Long] = new Array[Long](dfs.size)

    for(i <- 0 until  dfs.size){
      array(i) = dfs(i).toInt
    }

    array
  }

}
