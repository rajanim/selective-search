package org.sfsu.cs.io

import org.apache.spark.{SparkConf, SparkContext}
import org.sfsu.cs.clustering.kmeans.KMeanClustering
import org.sfsu.cs.index.IndexToSolr
import org.sfsu.cs.io.text.TextFileParser
import org.sfsu.cs.vectorize.VectorImpl
import util.TestSuiteBuilder

/**
  * Created by rajanishivarajmaski1 on 2/19/18.
  */
class TestQrelsDocsParser extends TestSuiteBuilder{

  test("qrelsDocsParser") {

    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("test app"))
    val tfDocs = TextFileParser.getTFDocuments(sc,
      "/Users/rajanishivarajmaski1/ClueWeb09_English_9/qrels_docs",
      20, "/Users/rajanishivarajmaski1/University/csc895/selective-search/src/test/resources/stopwords.txt")
    println(tfDocs.take(1).mkString(" "))
    val docVectors = VectorImpl.getDocVectors(sc, tfDocs, 10000)
    val result = KMeanClustering.train(data = docVectors.map(docVec => docVec.vector), 10, 5, 10000)
    IndexToSolr.indexToSolr(docVectors, "localhost:9983", "word-count", result)

  }
}
