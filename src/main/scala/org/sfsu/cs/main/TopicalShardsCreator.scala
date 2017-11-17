package org.sfsu.cs.main

import java.util.Calendar

import breeze.linalg.SparseVector
import com.lucidworks.spark.SparkApp.RDDProcessor
import org.apache.commons.cli.{CommandLine, Option}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.sfsu.cs.clustering.kmeans.KMeanClustering
import org.sfsu.cs.document.DocVector
import org.sfsu.cs.index.IndexToSolr
import org.sfsu.cs.io.clueweb09.Clueweb09Parser
import org.sfsu.cs.io.text.TextFileParser
import org.sfsu.cs.utils.Utility
import org.sfsu.cs.vectorize.VectorImpl
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by rajanishivarajmaski1 on 10/25/17.
  */
class TopicalShardsCreator extends RDDProcessor {

  val  logger: Logger= LoggerFactory.getLogger(TopicalShardsCreator.this.getName)
  def getName: String = "TopicalShardsCreator"

  var stopWordsFilePath = ""
  var numFeatures: Int = 0
  var numClusters: Int = 0
  var numIterations: Int = 0

  var numPartitions: Int = 0

  var warcFilesPath: String = null
  var textFilesPath: String = null

  var saveModelAt: String = null
  var modelPath: String = null
  var dictionaryLocation : String = null

  var collection: String = null
  var zkHost: String = null


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
        .desc("Directory path to save trained model")
        .longOpt("saveModelAt").build,
      Option.builder()
        .hasArg()
        .desc("Path to file containing list of stopwords separated by new line")
        .longOpt("stopWordsFilePath").build(),
      Option.builder("Train Phase/Project Phase")
        .hasArg()
        .desc("Input 0 or 1, 0 for train phase and 1 for project/test phase." +
          "For training phase, required input is path to training data set/files, " +
          "this phase will generate a model and dictionary and saves it at default or provided path" +
          "For project phase, required input is test data set path, modelPath and dictionaryLocation variables")
        .longOpt("trainTestPhaseFlg").build()

    )
  }

  def run(conf: SparkConf, cli: CommandLine): Int = {
    val sc = new SparkContext(conf)
    logger.warn("Spark context obtained", sc.sparkUser)
    warcFilesPath = cli.getOptionValue("warcFilesPath")
    if (warcFilesPath.isEmpty) {
      textFilesPath = cli.getOptionValue("textFilesPath")
      if (textFilesPath.isEmpty)
        throw new Exception(" Input files (warcFilesPath/textFilesPath path not found")
    }
    parseCmdLineArgsToVariables(cli)
    //work for clueweb warc files
    if (warcFilesPath != null && !warcFilesPath.isEmpty) {
      val stringDocs = Clueweb09Parser.getWarcRecordsViaFIS(sc, warcFilesPath, partitions = numPartitions)
      val tfDocs = Clueweb09Parser.getTFDocuments(sc, stringDocs, numPartitions, stopWordsFilePath)
      val docVectors = VectorImpl.getDocVectors(sc, tfDocs, numFeatures)
      val kMeansResult = initKmeans(docVectors)
      IndexToSolr.indexToSolr(docVectors, collection, zkHost, kMeansResult)
    } //work for text files
    else {
      val tfDocs = TextFileParser.getTFDocuments(sc, textFilesPath, numPartitions, stopWordsFilePath)
      val docVectors = VectorImpl.getDocVectors(sc, tfDocs, numFeatures)
      val kMeansResult = initKmeans(docVectors)
      IndexToSolr.indexToSolr(docVectors, collection, zkHost, kMeansResult)
    }
    0
  }

  /**
    * invoke kmeans for the vectors and return kmean centroids(array of vectors)
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
    numFeatures = cli.getOptionValue("numFeatures", "50").toInt
    numClusters = cli.getOptionValue("numClusters", "2").toInt
    numIterations = cli.getOptionValue("numIterations", "5").toInt

    numPartitions = cli.getOptionValue("numPartitions", "4").toInt

    warcFilesPath = cli.getOptionValue("warcFilesPath", "")
    textFilesPath = cli.getOptionValue("textFilesPath", "")


    val phase = cli.getOptionValue("phase", "0").toInt
    if(phase==1){
      modelPath = cli.getOptionValue("modelPath")
      dictionaryLocation = cli.getOptionValue("dictionaryLocation")
      if(modelPath.isEmpty || dictionaryLocation.isEmpty)
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

    //solr cloud values
    zkHost = cli.getOptionValue("zkHost", "localhost:9983")
    collection = cli.getOptionValue("collection", "collection1")
  }

  def createSparkContext(): SparkContext = {
    val sparkConf = new SparkConf().setAppName("TopicalShardsCreator")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(
      classOf[SparseVector[Int]],
      classOf[SparseVector[Long]],
      classOf[SparseVector[Double]],
      classOf[SparseVector[Float]]
    ))
    new SparkContext(sparkConf)
  }
}

