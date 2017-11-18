package org.sfsu.cs.search

import search.helper.ReDDEHelper
import search.query.ReDDESelecitveSearch
import util.TestSuiteBuilder

/**
  * Created by rajanishivarajmaski1 on 9/22/17.
  */
class TestReDDESelectiveSearch extends TestSuiteBuilder {


  test("getSampleDocsSize") {
    val numFound = new ReDDEHelper().getSampleDocsSize("news_byte_d_idf", 9,  "localhost:9983", 1)
    println(numFound)
  }


  test("buildCSIndex") {
    val response = new ReDDEHelper().buildCSIndex("news_byte_d_idf", "localhost:9983", 20, "csi_news_byte")
    println(response)
  }

  // "3cid",68
  // "12cid",20, "17cid",16,"18cid",12,"11cid",10,
  test("relevantDDEBasedSelectiveSearch_astronomy"){
    val response = new ReDDESelecitveSearch().relevantDDEBasedSelectiveSearch("localhost:9983", "csi_news_byte", "news_byte_d_idf", "astronomy", 1000, 1)
    for(i <- 0 until response.size()){
      println(response.get(i).get("clusterId_s"))
    }
  }

  // "4cid",101,  "16cid",41,
  // "3cid",57,"9cid",48,"20cid",46,"1cid",43,  "16cid",41,
  test("relevantDDEBasedSelectiveSearch_radio"){
    val response = new ReDDESelecitveSearch().relevantDDEBasedSelectiveSearch("localhost:9983", "csi_news_byte", "news_byte_d_idf", "radio", 1000, 1)
    for(i <- 0 until response.size()){
      println(response.get(i).get("clusterId_s"))
    }
  }

  //politics_mideast
  //"20cid",1326,"11cid",695,
  test("relevantDDEBasedSelectiveSearch_politics_mideast"){
    val response = new ReDDESelecitveSearch().relevantDDEBasedSelectiveSearch("localhost:9983", "csi_news_byte", "news_byte_d_idf", "politics mideast", 1000, 1)
    for(i <- 0 until response.size()){
      println(response.get(i).get("clusterId_s"))
    }
  }
}
