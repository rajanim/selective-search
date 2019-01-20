package org.sfsu.cs.io.csv

import java.util.{Calendar, UUID}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.sfsu.cs.document.{StringDocument, TFDocument}
import org.sfsu.cs.preprocess.CustomAnalyzer

/**
  * Created by rajani.maski on 8/2/17.
  */
object JobCSVFileReader {


  def main(args: Array[String]): Unit = {
    val stringDocs = JobCSVFileReader.readCSVGetTextCols( "/Users/rajani.maski/data_job_posts.csv")

    //CluewebReader.kmeanForJobsCSV(8000,20,stringDocs,75,15)

  }
  def readCSVGetTextCols(path : String) : RDD[StringDocument] ={

    val conf = new SparkConf().setMaster("local[*]").setAppName("JobCSVFileReader")
    val sc = SparkContext.getOrCreate(conf)

    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").
      option("inferSchema", "true").load(path)

    val text = df.select("Title", "JobDescription")

    val texts = for(item <- text.collect())yield {
      val stringBuilder : StringBuilder = new StringBuilder
      new StringDocument(UUID.randomUUID.toString,
        (stringBuilder.append(item.getString(0)).append(" ").append(item.getString(1)).toString()))
    }
     val filteredTexts = texts.filter(item => item.contents.trim.isEmpty)

   sc.parallelize(filteredTexts)

  }


  /**
    * Get tuple of file name(id) and tf map with term as key and tf count as value.
    * @param sc
    * @param stringDocs
    * @param partitions
    * @param stopWordsFilePath
    * @return
    */
  def getTFDocuments(sc: SparkContext, stringDocs: RDD[StringDocument], partitions: Int,  stopWordsFilePath: String): RDD[TFDocument] = {
    println(s"LOG: Start converting the raw data (html) to text: ${Calendar.getInstance().getTime()} ")
    val plainTextDocuments = stringDocs.map(doc ⇒ new StringDocument(doc.id, CustomAnalyzer.htmlToText(doc.contents)))
    println(s"LOG: End converting the raw data (html) to text: ${Calendar.getInstance().getTime()} ")
    val stopWords = sc.broadcast(scala.io.Source.fromFile(stopWordsFilePath).getLines().toSet).value
    println("stopwords: ", stopWords.take(10))
    CustomAnalyzer.initStem()
    val tfDocs = plainTextDocuments.map(doc => {
      val tf = CustomAnalyzer.tokenizeFilterStopWordsStem(doc.contents, stopWords)
      new TFDocument(doc.id, tf)
    })

    tfDocs

  }

}
