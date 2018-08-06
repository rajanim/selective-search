package org.sfsu.cs.io

import org.apache.spark.{SparkConf, SparkContext}
import org.sfsu.cs.clustering.kmeans.KMeanClustering
import org.sfsu.cs.index.IndexToSolr
import org.sfsu.cs.io.text.TextFileParser
import org.sfsu.cs.preprocess.CustomAnalyzer
import org.sfsu.cs.vectorize.VectorImpl
import util.TestSuiteBuilder

/**
  * Created by rajanishivarajmaski1 on 2/19/18.
  */
class TestQrelsDocsParser extends TestSuiteBuilder{

  test("qrelsDocsParser") {

    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("test app"))
    val tfDocs = TextFileParser.getTFDocuments(sc,
      "/Users/rajanishivarajmaski1/University/csc895/selective-search/src/test/resources/test_records/",
      20, "/Users/rajanishivarajmaski1/University/csc895/selective-search/src/test/resources/stopwords.txt")
    println(tfDocs.take(1).mkString(" "))
    val docVectors = VectorImpl.getDocVectors(sc, tfDocs, 50)
    val result = KMeanClustering.train(data = docVectors.map(docVec => docVec.vector), 3, 5, 50)
    IndexToSolr.indexToSolr(docVectors, "localhost:9983", "word-count", result)

  }


  test("customAnalyzer.stopwords"){
    val stopWords = Set[String]("the", "fieldposition", "del", "a", "nov", "termposition" ).toSet
    val map = CustomAnalyzer.tokenizeFilterStopWordsStem("14nov2008hotelfontainebleau20a the field_position download12mb", stopWords)

    map.foreach(println(_))

  }
}
