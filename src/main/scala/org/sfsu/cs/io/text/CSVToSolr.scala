package org.sfsu.cs.io.text

import java.io.{FileReader, IOException}
import java.util

import org.apache.commons.csv.CSVFormat
import com.lucidworks.spark.util.SolrSupport
import com.lucidworks.spark.util.SolrSupport.getCachedCloudClient
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions

/**
  * Created by rajani.maski on 7/13/17.
  */
object CSVToSolr {

  def main(args: Array[String]): Unit = {
    parseKaggleJobCSVIndexToSolr()
  }


  def parseCSVUsingSparkAPIS = {
    val sc = new SparkContext(new SparkConf().setAppName("parseCSVUsingSparkAPIS").setMaster("local[*]"))

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").
      option("inferSchema", "true").load("/Users/rajani.maski/data_job_posts.csv")
    println(df.columns)
    println(df.take(1))
    println(df.count())


  }


  /**
    * get job record in solr document object format
    * @param input
    * @return
    */
  def getSolrInputDoc(input: Row): SolrInputDocument = {
    val fields = "jobpost, Title"
    val seqString: Seq[String] = fields.split(",")
    println("writing input" + input.mkString(" "))
    println("writing string values: ", input.get(0), input.get(1))
    println("size", input.getValuesMap(seqString))
    val iter = input.getValuesMap(seqString).iterator
    val solrInputDocument: SolrInputDocument = new SolrInputDocument()
    while (iter.hasNext) {
      val keyValue = iter.next()
      solrInputDocument.addField(keyValue._1.toString, keyValue._2.toString)

    }
    println(solrInputDocument.toString)
    solrInputDocument

  }

  /**
    * Read input csv fils and iterate for each record
    *
    * @throws IOException
    */
  @throws[IOException]
  def parseKaggleJobCSVIndexToSolr() = {
    val in = new FileReader("/Users/rajani.maski/data_job_posts.csv")
    val records = CSVFormat.DEFAULT.withCommentMarker('#').withFirstRecordAsHeader
      .withIgnoreSurroundingSpaces.withIgnoreEmptyLines(true).parse(in).iterator()

    val solrInputDocs = new util.LinkedList[SolrInputDocument]

    while (records.hasNext) {
      val rec = records.next()
      solrInputDocs.add(getSolrInputDoc(rec.toMap))
    }


    val solrClient = getCachedCloudClient("localhost:9983")

    SolrSupport.sendBatchToSolr(solrClient, "jobs", JavaConversions.collectionAsScalaIterable(solrInputDocs), None)


  }

  def getSolrInputDoc(input: util.Map[String, String]): SolrInputDocument = {
    val iter = input.entrySet().iterator()
    val solrInputDocument: SolrInputDocument = new SolrInputDocument()
    while (iter hasNext) {
      val keyValue = iter.next()
      solrInputDocument.addField(keyValue.getKey.toString, keyValue.getValue.toString)
    }
    solrInputDocument

  }


}
