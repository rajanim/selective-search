package org.sfsu.cs.utils

import java.io.{BufferedWriter, File, FileOutputStream, FileWriter}
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.Date

import com.lucidworks.spark.util.SolrSupport
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.SparkContext
import org.sfsu.cs.preprocess.CustomAnalyzer

/**
  * Created by rajanishivarajmaski1 on 11/14/17.
  */
object Utility {

  def writeToFile(data: String, file: String) = {
    println("writeToFile", file)
    val output = new FileOutputStream(file)
    output.write(data.getBytes())
    output.close()

  }

  def appendResultsToFile(results: String, outFile: String) = {
    val file: File = new File(outFile)
    // if file doesnt exists, then create it
    if (!file.exists) file.createNewFile

    // true = append file
    val fw = new FileWriter(file.getAbsoluteFile, true)
    val bw = new BufferedWriter(fw)
    bw.write(results)
    bw.close()
    fw.close()
  }


  /**
    * Index batch of documents to solr
    *
    * @param zkHost
    * @param collectionName
    * @param solrInputDocs
    * @return
    */
  def indexToSolr(zkHost: String, collectionName: String, solrInputDocs: List[SolrInputDocument]): Int = {
    val solrClient: SolrClient = SolrSupport.getCachedCloudClient(zkHost)
    SolrSupport.sendBatchToSolr(solrClient, collectionName, (solrInputDocs))
    0
  }

  /*  def writeDataToFile(data:String) ={
      val file = getFilePath()
      writeDataToFile(data,file)
    }*/
  def getFilePath(): String = {
    val path = System.getProperty("user.dir") + "/spark_cluster_job_op/"
    if (!Files.exists(Paths.get(path)))
      Files.createDirectories(Paths.get(path))
    path + new SimpleDateFormat("yyyyMMdd'_'HHmmss").format(new Date)
  }

  def getCWDFilePath(): String = {
    val path = System.getProperty("user.dir") + "/"
    if (!Files.exists(Paths.get(path)))
      Files.createDirectories(Paths.get(path))
    path + new SimpleDateFormat("yyyyMMdd'_'HHmmss").format(new Date)
  }


  def getStopWords(filePath: String, sc: SparkContext): Set[String] = {
    if (!filePath.isEmpty) {
      sc.broadcast(scala.io.Source.fromFile(filePath).getLines().toSet).value
    } else {
      CustomAnalyzer.getStopWords()
    }
  }
}
