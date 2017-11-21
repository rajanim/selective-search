package org.sfsu.cs.io.csv

import org.sfsu.cs.clustering.kmeans.KMeanClustering
import org.sfsu.cs.document.StringDocument
import org.sfsu.cs.index.IndexToSolr
import org.sfsu.cs.main.SparkInstance
import org.sfsu.cs.vectorize.VectorImpl

/**
  * Created by rajanishivarajmaski1 on 11/21/17.
  */
object CSVFileRunner {

  def main(args: Array[String]): Unit = {
    val input = JobCSVFileReader.readCSVGetTextCols("")

    val sparkInstance = new SparkInstance
    val sc = sparkInstance.createSparkContext("JobsCsvFileRunner")
    val numPartitions = 20
    val numFeatures = 20000
    val k = 20
    val numIterations = 5
    val dirPath = "/Users/rajani.maski/data_job_posts.csv"

    sc.setLogLevel("WARN")
    val plainTextDocuments = input.map(doc ⇒ new StringDocument(doc.id, doc.contents))

    val tfDocs = JobCSVFileReader.getTFDocuments(sc,
      plainTextDocuments,
      numPartitions, "/Users/rajanishivarajmaski1/University/csc895/selective-search/src/test/resources/stopwords.txt")
    val docVectors = VectorImpl.getDocVectors(sc, tfDocs, numFeatures)
    val result = KMeanClustering.train(data = docVectors.map(docVec => docVec.vector), k, numIterations, numFeatures)
    IndexToSolr.indexToSolr(docVectors, "localhost:9983", "news_byte", result)

  }

}
