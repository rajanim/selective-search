package org.sfsu.cs.clustering.kmeans.distance

import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}


/**
  * Created by rajanishivarajmaski1 on 4/2/17.
  * fork from spark-scala apis
  * * The cosine distance between two points: cosineDistance(a,b) = (a dot b)/(norm(a) * norm(b))
  */


object CosineDistanceMeasure {

  def distance(v1: org.apache.spark.mllib.linalg.Vector, v2: org.apache.spark.mllib.linalg.Vector): Double = {
   // if (v1.size != v2.size) throw new CardinalityException(v1.size, v2.size)
    val dotProduct: Double = dot(v1, v2)
    val denom =  Vectors.norm(v1, 2.0) * Vectors.norm(v2, 2.0)
    if (denom == 0.0) {
      0.0
    } else {
      (dotProduct / denom)
    }
  }


  /**
    * dot(x, y)
    */
  def dot(x: Vector, y: Vector): Double = {
   val x1 = x.toDense
    val y1 = y.toDense

    //require(x.size == y.size,
    //  "BLAS.dot(x: Vector, y:Vector) was given Vectors with non-matching sizes:" +
      //  " x.size = " + x.size + ", y.size = " + y.size)
    (x1, y1) match {
      case (dx: DenseVector, dy: DenseVector) =>
        dot(dx, dy)
    //  case (sx: SparseVector, sy: SparseVector) =>
      //  dot(sx, sy)
      case _ =>
        throw new IllegalArgumentException(s"dot doesn't support (${x.getClass}, ${y.getClass}).")
    }
  }


  /**
    * L2Norm
    */
  def getL2Norm(values: Array[Double]): Double = {
    var sum = 0.0
    var i = 0
    val size = values.length
    while (i < size) {
      sum += values(i) * values(i)
      i += 1
    }
    math.sqrt(sum)
  }



  /**
    * dot(x, y) array of [double] of same size
    */
  def dot(v1: Array[Double], v2: Array[Double]): Double = {
    if (v1.size != v2.size) throw new CardinalityException(v1.size, v2.size)
    var result: Double = 0.0
    val max: Int = v1.size
    var i: Int = 0
    while (i < max) {
      result += v1(i) * v2(i)
      i += 1
    }
    result
  }


  /**
    * dot(x, y) DenseVector
    */
  def dot(v1: DenseVector, v2: DenseVector): Double = {

    if (v1.size != v2.size) throw new CardinalityException(v1.size, v2.size)
    var result: Double = 0.0
    val max: Int = v1.size
    var i: Int = 0
    while (i < max) {
      result += v1(i) * v2(i)
      i += 1
    }
    result
  }

  /**
    * dot(x, y) SparseVector
    */
  def dot(x: SparseVector, y: SparseVector): Double = {

    val xValues = x.values
    val xIndices = x.indices
    val yValues = y.values
    val yIndices = y.indices
    val nnzx = xIndices.length
    val nnzy = yIndices.length

    var kx = 0
    var ky = 0
    var sum = 0.0
    // y catching x
    while (kx < nnzx && ky < nnzy) {
      val ix = xIndices(kx)
      while (ky < nnzy && yIndices(ky) < ix) {
        ky += 1
      }
      if (ky < nnzy && yIndices(ky) == ix) {
        sum += xValues(kx) * yValues(ky)
        ky += 1
      }
      kx += 1
    }
    sum
  }





}

/**
  * Forked from spark-scala apis
  * Exception thrown when there is a cardinality mismatch in matrix or vector operations.
  * For example, vectors of differing cardinality cannot be added.
  */
class CardinalityException(val expected: Int, val cardinality: Int) extends IllegalArgumentException("Required cardinality " + expected + " but got " + cardinality) {
}