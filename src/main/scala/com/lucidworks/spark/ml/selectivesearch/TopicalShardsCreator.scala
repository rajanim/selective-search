package com.lucidworks.spark.ml.selectivesearch

import java.util.Calendar

import breeze.linalg.SparseVector
import breeze.numerics.log10
import com.lucidworks.spark.SparkApp.RDDProcessor
import org.apache.commons.cli.{CommandLine, Option}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.sfsu.cs.clustering.kmeans.KMeanClustering
import org.sfsu.cs.document.{DocVector, TFDocument}
import org.sfsu.cs.index.IndexToSolr
import org.sfsu.cs.io.clueweb09.Clueweb09Parser
import org.sfsu.cs.io.text.TextFileParser
import org.sfsu.cs.utils.Utility
import org.sfsu.cs.vectorize.VectorImpl
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
  * Created by rajanishivarajmaski1 on 10/25/17.
  */
class TopicalShardsCreator extends RDDProcessor {

  val logger: Logger = LoggerFactory.getLogger(TopicalShardsCreator.this.getName)

  def getName: String = "topicalshardscreator"

  var stopWordsFilePath = ""
  var minDocFreq=1
  var numFeatures: Int = 0
  var numClusters: Int = 0
  var numIterations: Int = 0

  var numPartitions: Int = 0

  var warcFilesPath: String = null
  var textFilesPath: String = null

  var saveModelAt: String = null
  var modelPath: String = null
  var dictionaryLocation: String = null

  var collection: String = null
  var zkHost: String = null
  var phase: Int = 0

  def getOptions: Array[Option] = {
    Array(
      Option.builder()
        .hasArg()
        .desc("Directory of clueweb 09 warc files")
        .longOpt("warcFilesPath").build,
      Option.builder()
        .hasArg()
        .desc("Directory of plain/html files")
        .longOpt("textFilesPath").build,
      Option.builder()
        .hasArg().required(false)
        .desc("Dictionary Location")
        .longOpt("dictionaryLocation").build,
      Option.builder()
        .hasArg()
        .desc("Queue size for ConcurrentUpdateSolrClient default is 1000")
        .longOpt("queueSize").build,
      Option.builder()
        .hasArg()
        .desc("Number of runner threads per ConcurrentUpdateSolrClient instance; default is 2")
        .longOpt("numRunners").build,
      Option.builder()
        .hasArg()
        .desc("Number of millis to wait until CUSS sees a doc on the queue before it closes the current request and starts another; default is 20 ms")
        .longOpt("pollQueueTime").build,
      Option.builder()
        .hasArg()
        .desc("Number of clusters documents should be grouped into, K value for KMean, defaults to 10")
        .longOpt("numClusters").build,
      Option.builder()
        .hasArg()
        .desc("Number of iterations for KMean, default to 20")
        .longOpt("numIterations").build,
      Option.builder()
        .hasArg()
        .desc("Number of features or dictionary terms to consider for vectorization, default to 50000")
        .longOpt("numFeatures").build,
      Option.builder()
        .hasArg()
        .desc("Path to KMean model is required for project phase\" +\n     " +
          "     \"by default, train phase is invoked, input documents are " +
          "considered as training dataset and model is generated using this dataset and \" +\n     " +
          "     \"records are then indexed to solr. For project phase, documents are vectorized based on dictionary terms and cluster is predicted")
        .longOpt("modelPath").build,
      Option.builder()
        .hasArg()
        .desc("Number of partitions for spark")
        .longOpt("numPartitions").build,
      Option.builder()
        .hasArg()
        .desc("min doc freq for vectors")
        .longOpt("minDocFreq").build,
      Option.builder()
        .hasArg()
        .desc("Directory path to save trained model")
        .longOpt("saveModelAt").build,
      Option.builder()
        .hasArg()
        .desc("Path to file containing list of stopwords separated by new line")
        .longOpt("stopWordsFilePath").build(),
      Option.builder()
        .hasArg()
        .desc("Input 0 or 1, 0 for train phase and 1 for project/test phase." +
          "For training phase, required input is path to training data set/files, " +
          "this phase will generate a model and dictionary and saves it at default or provided path" +
          "For project phase, required input is test data set path, modelPath and dictionaryLocation variables")
        .longOpt("trainTestPhaseFlg").build()

    )
  }

  def run(conf: SparkConf, cli: CommandLine): Int = {

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(
      classOf[SparseVector[Int]],
      classOf[SparseVector[Long]],
      classOf[SparseVector[Double]],
      classOf[SparseVector[Float]]
    ))
    val sc = new SparkContext(conf)
    logger.warn("Spark context obtained", sc.sparkUser)
    warcFilesPath = cli.getOptionValue("warcFilesPath")
    if (warcFilesPath == null || warcFilesPath.isEmpty) {
      textFilesPath = cli.getOptionValue("textFilesPath")
      if (textFilesPath == null || textFilesPath.isEmpty)
        throw new Exception(" Input files (warcFilesPath/textFilesPath path not found")
    }
    parseCmdLineArgsToVariables(cli)
    println("phase: ", phase)
    //work for clueweb warc files
    if (warcFilesPath != null && !warcFilesPath.isEmpty) {
      val stringDocs = Clueweb09Parser.getWarcRecordsViaSparkAPI(sc, warcFilesPath, partitions = numPartitions)
      stringDocs.cache()
      val tfDocs = Clueweb09Parser.getTFDocuments(sc, stringDocs, numPartitions, stopWordsFilePath)
      stringDocs.unpersist(true)
      tfDocs.cache()
      if (phase == 1) {
        executeProjectionPhase(sc, tfDocs)
      }
      else {
        val docVectors = VectorImpl.getDocVectors(sc, tfDocs, numFeatures, minDocFreq)
        tfDocs.unpersist(true)
        val kMeansResult = initKmeans(docVectors)
        IndexToSolr.indexToSolr(docVectors, zkHost, collection, kMeansResult)
      }
    } //work for text files
    else {
      val tfDocs = TextFileParser.getTFDocuments(sc, textFilesPath, numPartitions, stopWordsFilePath)
      if (phase == 1) {
        executeProjectionPhase(sc, tfDocs)
      }
      else {
        val docVectors = VectorImpl.getDocVectors(sc, tfDocs, numFeatures, minDocFreq)
        val kMeansResult = initKmeans(docVectors)
        IndexToSolr.indexToSolr(docVectors, zkHost, collection, kMeansResult)
      }
    }
    0
  }

  /**
    * Project documents to clusters utilizing the model/centroids generated during train phase.
    *
    * @param sc
    * @param tfDocs
    */
  def executeProjectionPhase(sc: SparkContext, tfDocs: RDD[TFDocument]): Unit = {
    val dict = readDict(dictionaryLocation, numFeatures)
    val idfs = sc.parallelize(dict.toSeq)
    val termIds = idfs.keys.zipWithIndex().collectAsMap()


    val bTermIds = sc.broadcast(termIds).value
    val bIdfs = sc.broadcast(idfs.collectAsMap()).value


    println(s"LOG: Start featurizing map to vectors: ${Calendar.getInstance().getTime()} ")

    val docVectors = tfDocs.map(doc => {
      val tfIdfMap = mutable.HashMap.empty[Int, Double]
      doc.tfMap.foreach(tfTuple => {
        if (bTermIds.contains(tfTuple._1)) {
          val tfidf = (1 + log10(tfTuple._2)) * bIdfs(tfTuple._1)
          val i = bTermIds(tfTuple._1)
          tfIdfMap.put(i.toInt, tfidf)

        }
      })
      new DocVector(doc.id, doc.tfMap, Vectors.sparse(numFeatures, tfIdfMap.toSeq))
    })

    println(s"LOG: End of featurizing map to vectors: ${Calendar.getInstance().getTime()} ")

    val kmeanCentroids = getCentroidsFromFile(modelPath)

    IndexToSolr.indexToSolr(docVectors, zkHost, collection, kmeanCentroids)

  }

  /**
    * Model
    * @param file
    * @return
    */
  def getCentroidsFromFile(file: String): Array[Vector] = {
    val vectors = scala.io.Source.fromFile(file).getLines()
    val array = new scala.collection.mutable.ArrayBuffer[Vector]()
    var i = 0
    vectors.foreach(str => {
      array += getVector(str)
      i += 1
    })
    array.toArray
  }

  def getVector(str: String): Vector = {
    Vectors.parse(str)

  }

  def readDict(file: String, n: Int = 10000000): Map[Any, Double] = {
    scala.io.Source.fromFile(file).getLines().take(n).map(str => {
      val arr = str.split("->")
      (arr(0).trim, arr(1).toDouble)
    }).toMap

  }

  /**
    * invoke kmeans for the vectors and return kmean centroids(array of vectors)
    *
    * @param docVectors
    * @return
    */
  def initKmeans(docVectors: RDD[DocVector]): Array[org.apache.spark.mllib.linalg.Vector] = {
    println(s"LOG: Start KMeanClustering: ${Calendar.getInstance().getTime()} ")
    val result = KMeanClustering.train(data = docVectors.map(docVec => docVec.vector), numClusters, numIterations, numFeatures)
    println(s"LOG: End KMeanClustering: ${Calendar.getInstance().getTime()} ")

    val spareCenters = result.map(vec => vec.toSparse)
    if (saveModelAt.isEmpty) {
      println("writing centroids to file location", Utility.getFilePath())
      Utility.writeToFile(spareCenters.mkString("\n"), Utility.getFilePath() + "_centroids")
    } else {
      println("writing centroids to file location", saveModelAt)
      Utility.writeToFile(spareCenters.mkString("\n"), saveModelAt)
    }
    result
  }


  def parseCmdLineArgsToVariables(cli: CommandLine) {
    stopWordsFilePath = cli.getOptionValue("stopWordsFilePath", "")
    minDocFreq = cli.getOptionValue("minDocFreq", "5").toInt
    numFeatures = cli.getOptionValue("numFeatures", "50").toInt
    numClusters = cli.getOptionValue("numClusters", "2").toInt
    numIterations = cli.getOptionValue("numIterations", "5").toInt

    numPartitions = cli.getOptionValue("numPartitions", "4").toInt

    warcFilesPath = cli.getOptionValue("warcFilesPath", "")
    textFilesPath = cli.getOptionValue("textFilesPath", "")


    phase = cli.getOptionValue("trainTestPhaseFlg", "0").toInt
    if (phase == 1) {
      modelPath = cli.getOptionValue("modelPath")
      dictionaryLocation = cli.getOptionValue("dictionaryLocation")
      if (modelPath.isEmpty || dictionaryLocation.isEmpty)
        throw new Exception("modelPath and dictionaryLocation required for predict/project phase")
    }
    saveModelAt = cli.getOptionValue("saveModel", "")
    logger.info(" Number of Features for Dictionary: " + numFeatures)
    logger.info(" Number of Clusters for KMean: " + numClusters)
    logger.info(" Number of Iterations for KMean: " + numIterations)
    logger.info(" Number of Partitions: " + numPartitions)
    logger.info(" warc Files directory path: " + warcFilesPath)
    logger.info(" text Files directory path: " + textFilesPath)
    logger.info(" model saved at: " + saveModelAt)
    logger.info("min doc freq", minDocFreq)

    //solr cloud values
    zkHost = cli.getOptionValue("zkHost", "localhost:9983")
    collection = cli.getOptionValue("collection", "collection1")
  }

}

