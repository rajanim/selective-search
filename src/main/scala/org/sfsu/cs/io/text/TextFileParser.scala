package org.sfsu.cs.io.text

import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.sfsu.cs.document.{StringDocument, TFDocument}
import org.sfsu.cs.preprocess.CustomAnalyzer

/**
  * Created by rajanishivarajmaski1 on 11/14/17.
  */
object TextFileParser {


  /**
    * Get tuple of file name(id) and tf map with term as key and tf count as value.
    * @param sc
    * @param url
    * @param partitions
    * @param stopWordsFilePath
    * @return
    */
  def getTFDocuments(sc: SparkContext, url: String, partitions: Int,  stopWordsFilePath: String): RDD[TFDocument] = {
    sc.setLogLevel("INFO")
    println(s"LOG: Start reading the raw data: ${Calendar.getInstance().getTime()} ")
    val htmlDocuments = sc.wholeTextFiles(url, partitions).map(x ⇒ new StringDocument(x._1, x._2))
    println(s"LOG: End reading the raw data: ${Calendar.getInstance().getTime()} ")

    println(s"LOG: Start converting the raw data (html) to text: ${Calendar.getInstance().getTime()} ")
    val plainTextDocuments = htmlDocuments.map(doc ⇒ new StringDocument(doc.id, CustomAnalyzer.htmlToText(doc.contents)))
    println(s"LOG: End converting the raw data (html) to text: ${Calendar.getInstance().getTime()} ")
    val stopWords = sc.broadcast(scala.io.Source.fromFile(stopWordsFilePath).getLines().toSet).value
    println("stopwords: ", stopWords.take(10))
    CustomAnalyzer.initStem()
    val tfDocs = plainTextDocuments.map(doc => {
      val tf = CustomAnalyzer.tokenizeFilterStopWordsStem(doc.contents, stopWords)
      new TFDocument(doc.id, tf)
    })

    tfDocs.cache()
    tfDocs

  }

  /**
    * Get tuple of file name and file content
    * @param sc
    * @param dirPath
    * @param partitions
    * @return
    */
  def getStringDocuments(sc: SparkContext, dirPath : String, partitions: Int) : RDD[StringDocument] = {
    sc.setLogLevel("INFO")
    println(s"LOG: Started reading the raw data: ${Calendar.getInstance().getTime()} ")
    sc.wholeTextFiles(dirPath, partitions).map(x ⇒ new StringDocument(x._1, x._2))
  }

}
