package org.sfsu.cs.index

import com.lucidworks.spark.util.SolrSupport
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.rdd.RDD
import org.sfsu.cs.clustering.kmeans.KMeanClustering
import org.sfsu.cs.document.DocVector

/**
  * Created by rajanishivarajmaski1 on 11/15/17.
  */
object IndexToSolr {


  /**
    *
    * @param docVectors
    * @param zkHost
    * @param collectionName
    * @param result
    */
  def indexToSolr(docVectors: RDD[DocVector],
                  zkHost: String, collectionName: String, result: Array[org.apache.spark.mllib.linalg.Vector]) = {
    val solrRecords = docVectors.map(doc => {
      getInputSolrDoc(doc, KMeanClustering.predict(result, doc.vector))
    })
    SolrSupport.indexDocs(zkHost, collectionName, 100, (solrRecords), Option(100000))

  }



  /**
    *
    * @param doc
    * @param clusterIdScore
    * @return
    */
  def getInputSolrDoc(doc: DocVector, clusterIdScore: (Int, Double)): SolrInputDocument = {
    val solrDoc = new SolrInputDocument
    val cid = Integer.valueOf(clusterIdScore._1) + 1
    solrDoc.addField("uid_s", cid + "cid" + "!_" + doc.id.substring(doc.id.lastIndexOf('/') + 1))
    solrDoc.addField("id", doc.id.substring(doc.id.lastIndexOf('/') + 1))
    solrDoc.addField("url_t", doc.id.split("[_]").mkString(" "))
    solrDoc.addField("content_t", doc.tfMap.keys.mkString(" "))
    solrDoc.addField("clusterId_s", cid + "cid")
    solrDoc.addField("clusterId_i", cid)
    solrDoc.addField("_route_", "shard" + cid)
    solrDoc.addField("similarityScore_s", clusterIdScore._2)

    //solrDoc.addField("raw_content_t", doc.contentText)
    solrDoc
  }
}
