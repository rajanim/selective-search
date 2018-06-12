package org.sfsu.cs.search.query

import com.lucidworks.spark.util.SolrQuerySupport
import com.lucidworks.spark.util.SolrSupport._
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.response.QueryResponse

/**
  * Created by rajanishivarajmaski1 on 6/4/18.
  */
object QueryUtil {

  def querySolrWithFq(zkHost: String, collection: String, searchQuery: String, fq:String, fields: String):  QueryResponse = {
    val solrClient = getCachedCloudClient(zkHost)
    val solrQuery = new SolrQuery(searchQuery)
    solrQuery.set("collection", collection)
    solrQuery.addFilterQuery(fq)
    solrQuery.setFields(fields)

    SolrQuerySupport.querySolr(solrClient, solrQuery, 0, null).get


  }
}
