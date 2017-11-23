package org.sfsu.cs.utils

import java.io.FileOutputStream
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.Date

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

  def writeToFile(data:String) ={
    val file = getFilePath()
    writeToFile(data,file)
  }
  def getFilePath(): String = {
    val path = System.getProperty("user.dir") + "/spark_cluster_job_op/"
    if(!Files.exists(Paths.get(path)))
      Files.createDirectories(Paths.get(path))
    path + new SimpleDateFormat("yyyyMMdd'_'HHmmss").format(new Date)
  }


  def getStopWords(filePath : String, sc : SparkContext) : Set[String] ={
    if(!filePath.isEmpty){
      sc.broadcast(scala.io.Source.fromFile(filePath).getLines().toSet).value
    }else{
      CustomAnalyzer.getStopWords()
    }
  }
}
