package org.sfsu.cs.io.text

import java.io.File
import java.util.Calendar

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.TrueFileFilter
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.sfsu.cs.document.{StringDocument, TFDocument}
import org.sfsu.cs.preprocess.CustomAnalyzer

import scala.collection.mutable.ListBuffer

/**
  * Created by rajanishivarajmaski1 on 11/14/17.
  */
object TextFileParser {


  /**
    * Get tuple of file name(id) and tf map with term as key and tf count as value.
    *
    * @param sc
    * @param url
    * @param partitions
    * @param stopWordsFilePath
    * @return
    */
  def getTFDocuments(sc: SparkContext, url: String, partitions: Int, stopWordsFilePath: String): RDD[TFDocument] = {
    sc.setLogLevel("INFO")
    println(s"LOG: Start reading the raw data: ${Calendar.getInstance().getTime()} ")
    val htmlDocuments = sc.wholeTextFiles(url, partitions).map(x ⇒ new StringDocument(x._1, x._2))
    println(s"LOG: End reading the raw data: ${Calendar.getInstance().getTime()} ")

    println(s"LOG: Start converting the raw data (html) to text: ${Calendar.getInstance().getTime()} ")
    val plainTextDocuments = htmlDocuments.map(doc ⇒ new StringDocument(doc.id, CustomAnalyzer.htmlToText(doc.contents)))
    println(s"LOG: End converting the raw data (html) to text: ${Calendar.getInstance().getTime()} ")
    val stopWords = sc.broadcast(scala.io.Source.fromFile(stopWordsFilePath).getLines().toSet).value
    println("stopwords: ", stopWordsFilePath, stopWords.size)
    CustomAnalyzer.initStem()
    val tfDocs = plainTextDocuments.map(doc => {
      val tf = CustomAnalyzer.tokenizeFilterStopWordsStem(doc.contents, stopWords)
      new TFDocument(doc.id, tf)
    })

    tfDocs

  }

  def getTFDocsForStringDocs(sc: SparkContext, stringDocs: RDD[StringDocument], partitions: Int, stopWordsFilePath: String): RDD[TFDocument] = {

    println(s"LOG: Start converting the raw data (html) to text: ${Calendar.getInstance().getTime()} ")
    val plainTextDocuments = stringDocs.map(doc ⇒ new StringDocument(doc.id, CustomAnalyzer.htmlToText(doc.contents)))
    println(s"LOG: End converting the raw data (html) to text: ${Calendar.getInstance().getTime()} ")
    val stopWords = sc.broadcast(scala.io.Source.fromFile(stopWordsFilePath).getLines().toSet).value
    println("stopwords: ", stopWordsFilePath, stopWords.size)
    CustomAnalyzer.initStem()
    val tfDocs = plainTextDocuments.map(doc => {
      val tf = CustomAnalyzer.tokenizeFilterStopWordsStem(doc.contents, stopWords)
      new TFDocument(doc.id, tf)
    })

    tfDocs

  }


  def getStringDocsViaIOUtils(sc: SparkContext, dirPath: String, partitions: Int): RDD[StringDocument] = {

    val filesIterator = FileUtils.iterateFilesAndDirs(new File(dirPath), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE)
    val rootList = new ListBuffer[StringDocument]()
    var dir: String = ""
    while (filesIterator.hasNext) {
      val file = filesIterator.next()
      if (!file.getName.contains(".DS_Store")) {
        if (file.isDirectory) {
          dir = file.getName
        }
        else {
          val fileText = FileUtils.readFileToString(file, "UTF-8")
          val fileName =  file.getName
            rootList.+=:(new StringDocument(fileName, fileText))
        }
      }
    }
    println("list size: " + rootList.size)
    sc.parallelize(rootList, partitions)
  }

  /**
    * Get tuple of file name and file content
    *
    * @param sc
    * @param dirPath
    * @param partitions
    * @return
    */
  def getStringDocuments(sc: SparkContext, dirPath: String, partitions: Int): RDD[StringDocument] = {
    sc.setLogLevel("INFO")
    println(s"LOG: Started reading the raw data: ${Calendar.getInstance().getTime()} ")
    sc.wholeTextFiles(dirPath, partitions).map(x ⇒ new StringDocument(x._1, x._2))
  }

}
