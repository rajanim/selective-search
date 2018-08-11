package org.sfsu.cs.search.helper

import com.lucidworks.spark.util.{SolrQuerySupport, SolrSupport}
import com.lucidworks.spark.util.SolrSupport._
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.common.{SolrDocument, SolrInputDocument}

import scala.collection.mutable.ListBuffer

/**
  * Created by rajanishivarajmaski1 on 11/17/17.
  */
class ReDDEHelper {

  /**
    * ReDDE requires central sample index(CSI) to be created before hand inorder to perform selective org.sfsu.cs.search.
    * CSI is 'x' percentage of documents selected uniformly in random from each cluster and central index created out of those docs.
    * @param clusteredDocsCollection
    * @param zkHost
    * @param clusterCount
    * @param csIndexCollection
    * @return
    */
  def buildCSIndex(clusteredDocsCollection: String, zkHost: String, clusterCount: Int, csIndexCollection: String, percent:Int): Int = {
    for (i <- 1 to clusterCount) {
      val numRows = getSampleDocsSize(clusteredDocsCollection, i, zkHost, percent)
      val solrQuery = new SolrQuery("*:*")
      solrQuery.set("collection", clusteredDocsCollection)
      solrQuery.setRows(numRows.toInt)
      solrQuery.setQuery("clusterId_s:" + i + "cid")
      solrQuery.addSort("random_" + scala.util.Random.nextInt(1000000), org.apache.solr.client.solrj.SolrQuery.ORDER.asc)
      println(solrQuery.toString)
      val solrClient = getCachedCloudClient(zkHost)
      println(solrQuery.toQueryString)
      val resp = SolrQuerySupport.querySolr(solrClient, solrQuery, 0, null)
      val results = resp.get.getResults
      println("results for cid", i, results.size)
      var solrInputDocumentList = new ListBuffer[SolrInputDocument]()
      for (j <- 0 until results.size()) {
        val solrDoc = results.get(j)
        solrInputDocumentList += getSolrInputDoc(solrDoc)
      }
      println("solrInputDocumentList", solrInputDocumentList.size)
      if(solrInputDocumentList.size>0)
      SolrSupport.sendBatchToSolr(solrClient, csIndexCollection, solrInputDocumentList, scala.Option(60))

    }
    0
  }

  /**
    *Resulting solr document converted back into solr input document object
    * @param solrDoc
    * @return
    */
  def getSolrInputDoc(solrDoc: SolrDocument): SolrInputDocument = {
    val inputDocument = new SolrInputDocument()
    val iter = solrDoc.entrySet().iterator()
    while (iter.hasNext) {
      val keyValue = iter.next()
      if (!keyValue.getKey.equalsIgnoreCase("_version_"))
        inputDocument.setField(keyValue.getKey, keyValue.getValue)
    }

    inputDocument
  }

  /**
    * calculate percentage of documents to sample from each cluster of large collection.
    * @param collectionName
    * @param cid
    * @param zkHost
    * @param percent
    * @return
    */
  def getSampleDocsSize(collectionName: String, cid: Int, zkHost: String, percent: Int): Long = {
    val q = scala.Option(new SolrQuery("clusterId_s:" + cid + "cid"))
    val numFound = SolrQuerySupport.getNumDocsFromSolr(collectionName, zkHost, q)
    ((percent * numFound.toDouble) / 100).round
  }

}
