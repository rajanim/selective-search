package org.sfsu.cs.io

import org.apache.spark.{SparkConf, SparkContext}
import org.sfsu.cs.clustering.kmeans.KMeanClustering
import org.sfsu.cs.index.IndexToSolr
import org.sfsu.cs.io.clueweb09.Clueweb09Parser
import org.sfsu.cs.vectorize.VectorImpl
import util.TestSuiteBuilder

/**
  * Created by rajanishivarajmaski1 on 11/13/17.
  */
class TestClueweb09Parser extends TestSuiteBuilder {


  test("getWarcRecords") {
    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("test app"))
    val stringDocs = Clueweb09Parser.getWarcRecordsViaFIS(sc, "/Users/rajanishivarajmaski1/ProjectsToTest/clueweb_20/", 4)
    println("stringDocs count", stringDocs.count)
    assertResult(37237)(stringDocs.count())
  }

  test("getTFDocuments") {
    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("test app"))
    val stringDocs = Clueweb09Parser.getWarcRecordsViaFIS(sc, "/Users/rajanishivarajmaski1/ProjectsToTest/clueweb_20/", 4)
    println("stringDocs count", stringDocs.count)
    assertResult(37237)(stringDocs.count())
    val tfDocs = Clueweb09Parser.getTFDocuments(sc, stringDocs, 4, "" )
    println(tfDocs.take(1).mkString(" "))
  }

  test("getDocVectors"){
    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("test app"))
    val stringDocs = Clueweb09Parser.getWarcRecordsViaFIS(sc, "/Users/rajanishivarajmaski1/ProjectsToTest/clueweb_20/", 4)
    println("stringDocs count", stringDocs.count)
    assertResult(37237)(stringDocs.count())
    val tfDocs = Clueweb09Parser.getTFDocuments(sc, stringDocs, 4, "" )
    println(tfDocs.take(1).mkString(" "))
    val docVectors = VectorImpl.getDocVectors(sc,tfDocs,20,2)
    println(docVectors.take(1).mkString(" "))
  }

  test("kmeans"){
    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("test app"))
    val stringDocs = Clueweb09Parser.getWarcRecordsViaFIS(sc, "/Users/rajanishivarajmaski1/ProjectsToTest/clueweb_20/", 4)
    println("stringDocs count", stringDocs.count)
    assertResult(37237)(stringDocs.count())
    val tfDocs = Clueweb09Parser.getTFDocuments(sc, stringDocs, 4, "" )
    println(tfDocs.take(1).mkString(" "))
    val docVectors = VectorImpl.getDocVectors(sc,tfDocs,20,2)
    val docVector = docVectors.first()
    val result = KMeanClustering.train(data = docVectors.map(docVec => docVec.vector), 10, 5, 20)
    println("docVector", docVector.tfMap.mkString(" "))
    println("clusterId and similarity measure", KMeanClustering.predict(result, docVector.vector))

  }

  test("indexTopicShardsToSolr"){

    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("test app"))
    val stringDocs = Clueweb09Parser.getWarcRecordsViaFIS(sc, "/Users/rajanishivarajmaski1/ProjectsToTest/clueweb_20/", 4)
    println("stringDocs count", stringDocs.count)
    assertResult(37237)(stringDocs.count())
    val tfDocs = Clueweb09Parser.getTFDocuments(sc, stringDocs, 4, "" )
    println(tfDocs.take(1).mkString(" "))
    val docVectors = VectorImpl.getDocVectors(sc,tfDocs,2000,2)
    val docVector = docVectors.first()
    val result = KMeanClustering.train(data = docVectors.map(docVec => docVec.vector), 10, 5, 2000)
    println("docVector", docVector.tfMap.mkString(" "))
    println("clusterId and similarity measure", KMeanClustering.predict(result, docVector.vector))
    IndexToSolr.indexToSolr(docVectors, "localhost:9983", "news_byte", result)

  }


}
