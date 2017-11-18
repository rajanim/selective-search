package org.sfsu.cs.clustering.kmeans.distance

import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.linalg.Vectors
import util.TestSuiteBuilder

/**
  * Created by rajanishivarajmaski1 on 4/2/17.
  */
class TestCosineDistanceMeasure extends TestSuiteBuilder {

  test("cosineDistance") {

    val vector1 = Vectors.sparse(5, Seq((1, 1.0), (2, 1.0)))

    val vector2 = Vectors.sparse(5, Seq((1, 1.0), (2, 1.0)))


    println("dot product", CosineDistanceMeasure.dot(vector1.toArray, vector2.toArray))
    println("Vectors.norm", Vectors.norm(vector2, 2.0))

    println("denom", Vectors.norm(vector1, 2.0) * Vectors.norm(vector2, 2.0))

    println(s"distance measure of vector 1 and 2, same", CosineDistanceMeasure.distance(vector1, vector2))

    val vector3 = Vectors.sparse(5, Seq((1, 1.0), (2, 1.0), (3, 1.0)))

    println(s"distance measure of vector 1 and 3, nearly similar", CosineDistanceMeasure.distance(vector1, vector3) )

    val vector4 = Vectors.sparse(5, Seq((0, 1.0), (1, 0.0), (2, 0.0), (3, 1.0)))

    println(s"distance measure of vector 1 and 4, dissimilar ", CosineDistanceMeasure.distance(vector1, vector4) )
  }

  test("cosineDistanceSparseVector"){
    val vec1 = collection.mutable.Map[Int, Double]()
    vec1.put(1, 1.0)
    vec1.put(2, 1.0)
    val vector1 = Vectors.sparse(5, vec1.toSeq)

    val vec2 = collection.mutable.Map[Int, Double]()
    vec2.put(1, 1.0)
    vec2.put(2, 1.0)
    vec2.put(3, 1.0)
    vec2.put(0, 1.0)
    val vector2 = Vectors.sparse(5, vec2.toSeq)
    println(vector1.toSparse, vector2.toSparse)
    println(s"distance measure of vector 1 and 2, same", CosineDistanceMeasure.distance(vector1, vector2))

  }

  test("cosineDistanceDenseVector"){
    val vec1 = collection.mutable.Map[Int, Double]()
    vec1.put(0, 1.0)
    vec1.put(1, 1.0)
    vec1.put(2, 1.0)
    vec1.put(3, 1.0)
    val vector1 = Vectors.dense(vec1.values.toArray)

    val vec2 = collection.mutable.Map[Int, Double]()
    vec2.put(1, 1.0)
    vec2.put(2, 1.0)
    vec2.put(3, 1.0)
    vec2.put(0, 1.0)
    val vector2 = Vectors.dense(vec2.values.toArray)
    println(vector1, vector2)
    println(s"distance measure of vector 1 and 2, same", CosineDistanceMeasure.distance(vector1, vector2))

    vec1.put(1, 0.0)
    val vector3 = Vectors.dense(vec1.values.toArray)
    println(vector1, vector3)
    println(s"distance measure of vector 1 and 2, same", CosineDistanceMeasure.distance(vector1, vector3))

  }



  test("cosineDistance between two Vectors with vector normalization.") {
    val normalize :  Normalizer  = new Normalizer()

    val vec1 = collection.mutable.Map[Int, Double]()
    vec1.put(1, 1.0)
    vec1.put(2, 1.0)
    val vector1 = Vectors.sparse(5, vec1.toSeq)


    val vec2 = collection.mutable.Map[Int, Double]()
    vec2.put(1, 1.0)
    vec2.put(2, 1.0)
    val vector2 = Vectors.sparse(5, vec2.toSeq)

    println(s"spark MLLib squared distance between two Vectors ${CosineDistanceMeasure.distance(vector1, vector2)}")

    vec1.put(3, 1.0)
    val vector3 = Vectors.sparse(5, vec1.toSeq)
    println(s"spark MLLib squared distance between two Vectors ${CosineDistanceMeasure.distance(normalize.transform(vector1), normalize.transform(vector3))}")

  }



}
