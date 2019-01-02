package org.sfsu.cs.misc

import java.io.{FileInputStream, InputStream}
import java.util.zip.GZIPInputStream

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.tika.io.IOUtils
import org.jwat.warc.{WarcReaderFactory, WarcRecord}
import org.sfsu.cs.io.clueweb.ResponseIterator
import org.sfsu.cs.io.clueweb.ResponseIterator.WarcEntry

import scala.collection.JavaConversions
import scala.collection.mutable.ListBuffer
import scala.io.Source

object ReadZipFilesViaBinariesAPI {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[1]").setAppName("readwarcfiles") //create a new spark config
    val sparkContext = new SparkContext(sparkConf) //build a spark context

    val out = sparkContext.binaryFiles("/Users/rajanishivarajmaski1/ClueWeb09_English_9/ClueWeb09_English_9/small", 1).flatMap {
      case (zipFilePath, content) =>


        //  val readerIter = WarcReaderFactory.getReader(content.open()).iterator()

        //val gzip = (new GZIPInputStream(content.open))
        //val entries: ResponseIterator = new ResponseIterator(gzip)
        val rootList = new ListBuffer[WarcEntry]()
        val inStream: InputStream = new GZIPInputStream(content.open())
        val entries: ResponseIterator = new ResponseIterator(inStream)
        while (entries.hasNext) rootList.+=:(entries.next())
        rootList

      //  val readerIter = WarcReaderFactory.getReader(gzip).iterator()


        //JavaConversions.asScalaIterator(entries.getIter)
        //val is = new ByteArrayInputStream(content.toArray())
        //val entries: ResponseIterator = new ResponseIterator(is)
      //  val rootList = new ListBuffer[WarcRecord]()

        //println("trec id ", entries.computeNext.trecId)

       // while (readerIter.hasNext) {
         // rootList.+=:(readerIter.next())
        //}

        //rootList
      //JavaConversions.asScalaIterator(entries.getIter)
    }

    // out.collect().foreach(warc => println(warc.getHttpHeader))

    println("printing size")
    println(out.collect().size)
    out.collect().foreach(f=> println(f.trecId))
    //out.collect().foreach(println(_))

    /*val htmlDocuments = out
      .map ( record => {

        val id = record.getHeader("WARC-TREC-ID").value
        val html = IOUtils.toString(record.getPayloadContent, "UTF-8")
        new StringDocument(id, html)
    })

   println (htmlDocuments.collect().size)

    // Map each HTML document to its plain text equivalent
    htmlDocuments.map(doc ⇒ new StringDocument(doc.id, doc.contents))


    val doc = htmlDocuments.collect()

    println(doc.size)

    doc.foreach(stringp => println(stringp.id))
*/
  }
}

/*


val out = sparkContext.binaryFiles("/Users/rajanishivarajmaski1/ClueWeb09_English_9/ClueWeb09_English_9/small", 2).flatMap {
  case (zipFilePath, content) =>
  val reader = WarcReaderFactory.getReader(new ByteArrayInputStream(content.toArray()))
  JavaConversions.asScalaIterator(reader.iterator())
}*/
