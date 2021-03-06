package org.sfsu.cs.search

import org.sfsu.cs.search.helper.CORIHelper
import org.sfsu.cs.search.query.CORISelectiveSearchFileIndex
import util.TestSuiteBuilder

/**
  * Created by rajanishivarajmaski1 on 10/12/17.
  */
class TestCoriSelectiveSearch extends TestSuiteBuilder {


  test("testCoriSelectiveSearch") {

    val coriSelectiveSearch = new CORISelectiveSearchFileIndex()
    coriSelectiveSearch.initCoriSelectiveSearch("/Users/rajanishivarajmaski1/University/csc895/selective-org.sfsu.cs.search/spark_cluster_job_op/20180326_152619")
    val searchQuery = "zucsoi"
    val fieldToret = "clusterId_s,score, content_t,id"
    val docs = coriSelectiveSearch.executeCoriSelectiveSearch("localhost:9983", "news_byte_d_idf", searchQuery, 5, 0.4, fieldToret).iterator()
    println("printing cori results for org.sfsu.cs.search query:" + searchQuery)
    while (docs.hasNext){
      println(docs.next().toString)
    }

  }

  test("readCORIParamsFromFile") {
    new CORIHelper()
      .readCORIParamsFromFile("/Users/rajanishivarajmaski1/University/spark-solr-899/spark-solr/spark_cluster_job_op/20171023_134721")

  }

  test("getCoriCwAvgCwParams") {

    new CORIHelper().getCoriCwAvgCwParams("localhost:9983", "Test")

  }
  test("logTermDFIndexForCori") {
    new CORIHelper().getTermDFIndexForCori("Test", "localhost:9983", 2)

  }

  test("logTermDFIndexCwAvgCwForCORI") {
    new CORIHelper().logTermDFIndexCwAvgCwForCORI("news_byte_d_idf", "localhost:9983", 20)

  }


}
