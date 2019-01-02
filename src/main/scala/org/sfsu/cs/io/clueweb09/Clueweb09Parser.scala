package org.sfsu.cs.io.clueweb09

import java.io._
import java.util.Calendar
import java.util.zip.GZIPInputStream

import org.apache.commons.io.IOUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.jwat.warc.{WarcReaderFactory, WarcRecord}
import org.sfsu.cs.document.{StringDocument, TFDocument}
import org.sfsu.cs.io.clueweb.ResponseIterator
import org.sfsu.cs.io.clueweb.ResponseIterator.WarcEntry
import org.sfsu.cs.preprocess.CustomAnalyzer
import org.sfsu.cs.utils.Utility

import scala.collection.JavaConversions
import scala.collection.mutable.ListBuffer

/**
  * Created by rajanishivarajmaski1 on 10/27/17.
  */
object Clueweb09Parser {


  /**
    * get tuple of warc record with tfmap which has key as term and value as term frequency.
    * @param sc
    * @param input
    * @param partitions
    * @param stopWordsFilePath
    * @return
    */
  def getTFDocuments(sc: SparkContext, input : RDD[StringDocument], partitions: Int,  stopWordsFilePath: String): RDD[TFDocument] = {
    sc.setLogLevel("WARN")
    println(s"LOG: Start converting the raw data (html) to text: ${Calendar.getInstance().getTime()} ")
    val plainTextDocuments = input.map(doc ⇒ new StringDocument(doc.id, CustomAnalyzer.htmlToText(doc.contents)))
    println(s"LOG: End converting the raw data (html) to text: ${Calendar.getInstance().getTime()} ")
    val stopWords = Utility.getStopWords(stopWordsFilePath,sc)
    println("stopwords: ", stopWords.take(10))
    CustomAnalyzer.initStem()
    val tfDocs = plainTextDocuments.map(doc => {
      val tf = CustomAnalyzer.tokenizeFilterStopWordsStem(doc.contents, stopWords)
      new TFDocument(doc.id, tf)
    })
    tfDocs
  }



    /**
    *Get warc records from given directory file path, this method uses java's file util libraries to process warc records
    * @param sc
    * @param dirPath
    * @param partitions
    * @return
    */
  def getWarcRecordsViaFIS(sc: SparkContext, dirPath: String, partitions: Int): RDD[StringDocument] = {
    val rootList = new ListBuffer[ResponseIterator.WarcEntry]()
    val file: File = new File(dirPath)
    val inputFiles: Array[File] = file.listFiles
    for (inputFile <- inputFiles) {
      if (inputFile.getName.endsWith("warc.gz")) {
        val inStream: InputStream = new GZIPInputStream(new FileInputStream(inputFile))
        val entries: ResponseIterator = new ResponseIterator(inStream)
        while (entries.hasNext) {
          rootList.+=:(entries.next())
        }
      }
    }
    val totalSize = rootList.size
    println("list size: " + totalSize)
   val ret = sc.parallelize(rootList, partitions).map(record => new StringDocument(record.trecId, IOUtils.toString(record.content, "UTF-8")))
  rootList.remove(totalSize-1)
    ret
  }


  /**
    * Gets the documents from given warc records directory path, this method uses sparks whole text file library to process warc files.
    * from https://github.com/rjagerman/mammoth/blob/master/src/main/scala/ch/ethz/inf/da/mammoth/io/CluewebReader.scala
    * @param sc The spark context
    * @param input The input
    * @return
    *  note : this method has issue reported here : https://bugs.openjdk.java.net/browse/JDK-8154035
    *  so we have above method {getWarcRecordsViaFIS} that reads warc files via java file input stream.
    */
  def getWarcRecordsViaSparkAPI(sc:SparkContext, input:String, partitions:Int): RDD[StringDocument] = {

    // Get the warc records
    val warcRecords =  getWarcRecordsFromDirectory(sc, input, partitions)

    // Filter out records that are not reponses and get the HTML contents from the remaining WARC records
    val htmlDocuments = warcRecords.
      filter {
      record => try {
        record.warcType.trim == "response"
      } catch {
        case e: Exception => false
      }
    }
    .map {
      record =>
        val id = record.trecId
        val html = IOUtils.toString(record.content, "UTF-8")
        new StringDocument(id, html)
    }

    // Map each HTML document to its plain text equivalent
    htmlDocuments.map(doc ⇒ new StringDocument(doc.id, doc.contents))

  }

  /**
    * Gets WARC records from files in a directory
    *
    * @param sc The spark context
    * @param input The directory where the files are located
    * @return An RDD of the WARC records
    */
  def getWarcRecordsFromDirectory(sc:SparkContext, input:String, partitions:Int): RDD[WarcEntry] = {

    sc.binaryFiles(input,partitions).flatMap(x =>
      {
        val rootList = new ListBuffer[WarcEntry]()
        val inStream: InputStream = new GZIPInputStream(x._2.open)
        val entries: ResponseIterator = new ResponseIterator(inStream)
        while (entries.hasNext) rootList.+=:(entries.next())
        inStream.close()
        rootList
      }
    )

    // sc.wholeTextFiles(input, partitions).flatMap(x => getWarcRecordsFromString(x._2))
  }

  /**
    * Extracts WARC records from a file
    *
    * @param contents The contents of the WARC file as a string
    * @return An iterator of WarcRecords
    */
  def getWarcRecordsFromString(contents: String): Iterator[WarcRecord] = {
    //val reader = WarcReaderFactory.getReader(contents)

    val reader = WarcReaderFactory.getReader(new ByteArrayInputStream(contents.getBytes("UTF-8")))
    JavaConversions.asScalaIterator(reader.iterator())
  }

}
