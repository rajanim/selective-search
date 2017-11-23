package org.sfsu.cs.io.newsgroup

import org.sfsu.cs.clustering.kmeans.KMeanClustering
import org.sfsu.cs.index.IndexToSolr
import org.sfsu.cs.io.text.TextFileParser
import org.sfsu.cs.main.SparkInstance
import org.sfsu.cs.utils.Utility
import org.sfsu.cs.vectorize.VectorImpl

/**
  * Created by rajanishivarajmaski1 on 11/17/17.
  * Stopwords under /src/test/resources/stopwords.txt
  */
object NewsgroupRunner {

  def main(args: Array[String]): Unit = {
    val sparkInstance = new SparkInstance
    val sc = sparkInstance.createSparkContext("NewsByte")
    val numPartitions = 20
    val numFeatures = 20000
    val k = 20
    val numIterations = 5
    val dirPath = "/Users/rajanishivarajmaski1/University/CSC849_Search/20news-bydate/20news-18828_in_1"

    val tfDocs = TextFileParser.getTFDocuments(sc,
      dirPath,
      numPartitions, "/Users/rajanishivarajmaski1/University/csc895/selective-search/src/test/resources/stopwords.txt")
    val docVectors = VectorImpl.getDocVectors(sc, tfDocs, numFeatures)
    val result = KMeanClustering.train(data = docVectors.map(docVec => docVec.vector), k, numIterations, numFeatures)
    Utility.writeToFile(result.mkString("\n"), Utility.getFilePath() + "_centroids")
    IndexToSolr.indexToSolr(docVectors, "localhost:9983", "news_byte", result)
  }

}
