package org.sfsu.cs


import java.util.Calendar

import breeze.linalg.SparseVector
import com.lucidworks.spark.SparkApp.RDDProcessor
import org.apache.commons.cli.{CommandLine, Option}
import org.apache.spark.{SparkConf, SparkContext}
import org.sfsu.cs.io.clueweb09.Clueweb09Parser

/**
  * Created by rajanishivarajmaski1 on 10/25/17.
  */
class TopicalShardsCreation extends RDDProcessor {

  def getName: String = "TopicalShardsCreation"

  var numFeatures: Int = 0
  var numClusters: Int = 0
  var numIterations: Int = 0

  var numPartitions: Int = 0

  var warcFilesPath: String = null
  var textFilesPath: String = null

  var saveModelAt: String = null
  var modelPath: String = null

 var collection: String = null
  var zkHost: String = null
var stopWordsFilePath = ""

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
    warcFilesPath = cli.getOptionValue("warcFilesPath")
    if (warcFilesPath == null || warcFilesPath.isEmpty) {
      textFilesPath = cli.getOptionValue("textFilesPath")
      if (textFilesPath == null || textFilesPath.isEmpty)
        throw new Exception(" Input files (warcFilesPath/textFilesPath path not found")
    }

    parseCmdLineAssignToVariables(cli)

    //work for clueweb warc files
    if (warcFilesPath != null && !warcFilesPath.isEmpty) {
      println(s"LOG: Start reading the raw data: ${Calendar.getInstance().getTime()} ")
      val stringDocs = Clueweb09Parser.getWarcRecordsViaFIS(sc, warcFilesPath, partitions = numPartitions)
      println(s"LOG: End reading the raw data via java file io: ${Calendar.getInstance().getTime()} ")
      val tfDocs = Clueweb09Parser.getTFDocuments(sc, stringDocs, numPartitions, stopWordsFilePath)
      tfDocs.cache()


    } //work for text files
    else {


    }

    0
  }

  def parseCmdLineAssignToVariables(cli: CommandLine) {
    //check training or test phase. For test phase, model path is required.
    var modelPath: String = null

    numFeatures = cli.getOptionValue("numFeatures", "50").toInt
    numClusters = cli.getOptionValue("numClusters", "2").toInt
    numIterations = cli.getOptionValue("numIterations", "5").toInt
    numPartitions = cli.getOptionValue("numPartitions", "10").toInt
    warcFilesPath = cli.getOptionValue("warcFilesPath")
    textFilesPath = cli.getOptionValue("textFilesPath")
    saveModelAt = cli.getOptionValue("saveModel", System.getProperty("user.dir") + "/Spark_Trained_Model/")
    stopWordsFilePath = cli.getOptionValue("stopWordsFilePath")
    //todo : do a null check for model path and dictionary location  incase phase = 1 and report exception.
    modelPath = cli.getOptionValue("modelPath")
    //dictionaryLocation = cli.getOptionValue("dictionaryLocation")


    println(" Number of Features for Dictionary: " + numFeatures)
    println(" Number of Clusters for KMean: " + numClusters)
    println(" Number of Iterations for KMean: " + numIterations)
    println(" Number of Partitions: " + numPartitions)
    println(" warc Files directory path: " + warcFilesPath)

    //solr cloud values
    zkHost = cli.getOptionValue("zkHost", "localhost:9983")
    collection = cli.getOptionValue("collection", "collection1")
  }

  def createSparkContext(): SparkContext = {
    val sparkConf = new SparkConf().setAppName("TopicalShardsCreation")
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

