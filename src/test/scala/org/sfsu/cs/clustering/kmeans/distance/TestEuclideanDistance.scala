package org.sfsu.cs.clustering.kmeans.distance

import org.apache.commons.math3.ml.distance.EuclideanDistance
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.linalg.Vectors
import util.TestSuiteBuilder

/**
  * Created by rajanishivarajmaski1 on 4/3/17.
  */
class TestEuclideanDistance extends TestSuiteBuilder{

  test("eucledianDistance") {
    val vec1 = collection.mutable.Map[Int, Double]()
    vec1.put(1, 1.0)
    vec1.put(2, 1.0)
    val vector1 = Vectors.sparse(5, vec1.toSeq)
    val vec2 = collection.mutable.Map[Int, Double]()
    vec2.put(1, 1.0)
    vec2.put(2, 1.0)
    val vector2 = Vectors.sparse(5, vec1.toSeq)

    val euclideanDistance = new EuclideanDistance
    euclideanDistance.compute(vector1.toArray, vector2.toArray)

    println("distance measure of vector 1 and 2, same", euclideanDistance.compute(vector1.toArray, vector2.toArray))

    vec1.put(3, 1.0)
    val vector3 = Vectors.sparse(5, vec1.toSeq)
    println("distance measure of vector 1 and 3, nearly similar", euclideanDistance.compute(vector1.toArray, vector3.toArray))

    vec1.clear()
    vec1.put(4, 1.0)
    val vector4 = Vectors.sparse(5, vec1.toSeq)
    println("distance measure of vector 1 and 4, dissimilar ", euclideanDistance.compute(vector1.toArray, vector4.toArray))

  }

  test("spark MLLib squared distance between two Vectors.") {
    val vec1 = collection.mutable.Map[Int, Double]()
    vec1.put(1, 1.0)
    vec1.put(2, 1.0)
    val vector1 = Vectors.sparse(5, vec1.toSeq)
    val vec2 = collection.mutable.Map[Int, Double]()
    vec2.put(1, 1.0)
    vec2.put(2, 1.0)
    val vector2 = Vectors.sparse(5, vec1.toSeq)

    println(s"spark MLLib squared distance between two Vectors ${Vectors.sqdist(vector1, vector2)}")

    vec1.put(3, 1.0)
    val vector3 = Vectors.sparse(5, vec1.toSeq)
    println(s"spark MLLib squared distance between two Vectors ${Vectors.sqdist(vector1, vector3)}")

  }

  test("spark MLLib Normalize vectors and compute squared  distance between two Vectors.") {
    val normalize :  Normalizer  = new Normalizer()

    val vec1 = collection.mutable.Map[Int, Double]()
    vec1.put(1, 1.0)
    vec1.put(2, 1.0)
    val vector1 = Vectors.sparse(5, vec1.toSeq)


    val vec2 = collection.mutable.Map[Int, Double]()
    vec2.put(1, 1.0)
    vec2.put(2, 1.0)
    val vector2 = Vectors.sparse(5, vec1.toSeq)

    println(s"spark MLLib squared distance between two Vectors ${Vectors.sqdist(vector1, vector2)}")

    vec1.put(3, 1.0)
    val vector3 = Vectors.sparse(5, vec1.toSeq)
    println(s"spark MLLib squared distance between two Vectors ${Vectors.sqdist(normalize.transform(vector1), normalize.transform(vector3))}")

  }

}
