package org.sfsu.cs.main

import breeze.linalg.SparseVector
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by rajanishivarajmaski1 on 11/17/17.
  */
class SparkInstance extends Serializable{

  def createSparkContext(appName : String): SparkContext = {
    val sparkConf = new SparkConf().setAppName(appName).setMaster("local[*]")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(
      classOf[SparseVector[Int]],
      classOf[SparseVector[Long]],
      classOf[SparseVector[Double]],
      classOf[SparseVector[Float]]
    ))
    SparkContext.getOrCreate(sparkConf)
  }

}
