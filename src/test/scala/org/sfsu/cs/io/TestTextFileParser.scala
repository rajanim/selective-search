package org.sfsu.cs.io

import org.apache.spark.{SparkConf, SparkContext}
import org.sfsu.cs.clustering.kmeans.KMeanClustering
import org.sfsu.cs.index.IndexToSolr
import org.sfsu.cs.io.text.TextFileParser
import org.sfsu.cs.vectorize.VectorImpl
import util.TestSuiteBuilder

/**
  * Created by rajanishivarajmaski1 on 11/14/17.
  */
class TestTextFileParser extends TestSuiteBuilder {

  test("getTfDocuments") {
    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("test app"))
    val tfDocs = TextFileParser.getTFDocuments(sc,
      "/Users/rajanishivarajmaski1/University/csc895/selective-org.sfsu.cs.search/src/test/resources/test_records",
      2, "/Users/rajanishivarajmaski1/University/csc895/selective-org.sfsu.cs.search/src/test/resources/stopwords.txt")
    println(tfDocs.take(1).mkString(" "))

  }

  test("kmeans") {

    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("test app"))
    val tfDocs = TextFileParser.getTFDocuments(sc,
      "/Users/rajanishivarajmaski1/University/csc895/selective-search/src/test/resources/test_records/",
      2, "/Users/rajanishivarajmaski1/University/csc895/selective-search/src/test/resources/stopwords.txt")
    println(tfDocs.take(1).mkString(" "))
    val docVectors = VectorImpl.getDocVectors(sc, tfDocs, 20)
    val docVector = docVectors.first()
    val result = KMeanClustering.train(data = docVectors.map(docVec => docVec.vector), 3, 5, 20)
    println("docVector", docVector.tfMap.mkString(" "))
    println("clusterId and similarity measure", KMeanClustering.predict(result, docVector.vector))
    IndexToSolr.indexToSolr(docVectors, "localhost:9983", "word-count", result)

  }
}
