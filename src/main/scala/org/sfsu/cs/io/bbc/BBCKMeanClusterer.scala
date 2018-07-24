package org.sfsu.cs.io.bbc

import org.sfsu.cs.clustering.kmeans.KMeanClustering
import org.sfsu.cs.index.IndexToSolr
import org.sfsu.cs.io.text.TextFileParser
import org.sfsu.cs.main.SparkInstance
import org.sfsu.cs.utils.Utility
import org.sfsu.cs.vectorize.VectorImpl

/**
  * Created by rajanishivarajmaski1 on 7/22/18.
  */
object BBCKMeanClusterer {

  def main(args: Array[String]): Unit = {
    val sparkInstance = new SparkInstance
    val sc = sparkInstance.createSparkContext("BBC")
    val numPartitions = 5
    val numFeatures = 4000
    val k = 5
    val numIterations = 10
    val dirPath = "/Users/rajanishivarajmaski1/University/bbc_dataset/bbcsport_all/"
    val stopWords = "/Users/rajanishivarajmaski1/University/csc895/selective-search/src/test/resources/stopwords.txt"
    val stringDocs = TextFileParser.getStringDocsViaIOUtils(sc,
      dirPath,
      numPartitions)
    val tfDocs = TextFileParser.getTFDocsForStringDocs(sc, stringDocs, partitions = numPartitions, stopWordsFilePath = stopWords)
    val docVectors = VectorImpl.getDocVectors(sc, tfDocs, numFeatures)
    val result = KMeanClustering.train(data = docVectors.map(docVec => docVec.vector), k, numIterations, numFeatures)
    Utility.writeToFile(result.mkString("\n"), Utility.getFilePath() + "_centroids")
    IndexToSolr.indexToSolr(docVectors, "localhost:9983", "clueweb_s", result)
  }

}
