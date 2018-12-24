package org.sfsu.cs.vectorize

import java.util.Calendar

import breeze.numerics._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.sfsu.cs.document.{DocVector, TFDocument}
import org.sfsu.cs.utils.Utility

import scala.collection.mutable

/**
  * Created by rajanishivarajmaski1 on 11/15/17.
  */
object VectorImpl {


  /**
    * Obtain TF-IDF based document vectors for input docs. Vector size is equivalent to numFeatures (dictionary size) set.
    *
    * @param sc
    * @param tfDocs
    * @param numFeatures
    * @return
    */
  def getDocVectors(sc: SparkContext, tfDocs: RDD[TFDocument], numFeatures : Int, minDocFreq: Int) : RDD[DocVector]= {
    val numDocs = tfDocs.count()
    println(s"LOG: End numDocs count: $numDocs, at time ${Calendar.getInstance().getTime()} ")
   // println(s"LOG: Start docFreqs map builder: ${Calendar.getInstance().getTime()} ")

    val docFreqs = tfDocs.map(tfDoc => tfDoc.tfMap)
      .flatMap(_.keySet).map((_, 1)).reduceByKey(_ + _)

    //println(s"LOG: End docFreqs map builder: ${Calendar.getInstance().getTime()} ")
    println(s"doc freq size: ", docFreqs.count())

    val ordering = Ordering.by[(Any, Int), Int](_._2)
    val topDocFreqs = docFreqs.top(numFeatures)(ordering).filter(_._2>minDocFreq)
    println(s"LOG: End topDocFreqs map builder: ${Calendar.getInstance().getTime()} ")
    println(s"topDocFreqs: ")
    //topDocFreqs.foreach(println(_))
    val rddTopDocFreqs = sc.parallelize(topDocFreqs)

    val idfs = rddTopDocFreqs.map {
      case (term, count) => (term, math.log(numDocs.toDouble / count))
    }

    val termIds = idfs.keys.zipWithIndex().collectAsMap()

    val bTermIds = sc.broadcast(termIds).value
    val bIdfs = sc.broadcast(idfs.collectAsMap()).value
    Utility.writeToFile(bIdfs.toMap.mkString("\n"), Utility.getFilePath() + "idfMap")


    println(s"LOG: Start featurizing map to vectors: ${Calendar.getInstance().getTime()} ")

    val docVectors = tfDocs.map(doc => {
      val tfIdfMap = mutable.HashMap.empty[Int, Double]
      doc.tfMap.foreach(tfTuple => {
        if (bTermIds.contains(tfTuple._1)) {
          val tfidf = (1 + log10(tfTuple._2)) * bIdfs(tfTuple._1)
          val i = bTermIds(tfTuple._1)
          tfIdfMap.put(i.toInt, tfidf)

        }
      })
      new DocVector(doc.id, doc.tfMap, Vectors.sparse(numFeatures, tfIdfMap.toSeq))
    })

    println(s"LOG: End of featurizing map to vectors: ${Calendar.getInstance().getTime()} ")

    docVectors

  }
}
