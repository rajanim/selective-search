package org.sfsu.cs.clustering.kmeans.distance

import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by rajanishivarajmaski1 on 10/5/17.
  */
object EuclideanDistanceMeasure {

  def distance(v1: org.apache.spark.mllib.linalg.Vector, v2: org.apache.spark.mllib.linalg.Vector): Double = {
    val  sqDist = Vectors.sqdist(v1, v2)
    math.sqrt(sqDist)
  }

  }
