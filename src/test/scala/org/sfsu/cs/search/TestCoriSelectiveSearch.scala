package org.sfsu.cs.search

import search.helper.CORIHelper
import search.query.CORISelectiveSearch
import util.TestSuiteBuilder

/**
  * Created by rajanishivarajmaski1 on 10/12/17.
  */
class TestCoriSelectiveSearch extends TestSuiteBuilder {


  test("testCoriSelectiveSearch") {

    val coriSelectiveSearch = new CORISelectiveSearch()
    coriSelectiveSearch.initCoriSelectiveSearch("/Users/rajanishivarajmaski1/University/spark-solr-899/spark-solr/spark_cluster_job_op/20171023_134721")
    coriSelectiveSearch.executeCoriSelectiveSearch("localhost:9983", "Test", "solr", 0.4)

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
    new CORIHelper().logTermDFIndexCwAvgCwForCORI("Test", "localhost:9983", 2)

  }


}
