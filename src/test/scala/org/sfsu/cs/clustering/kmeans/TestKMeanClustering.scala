package org.sfsu.cs.clustering.kmeans

import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vectors}
import org.apache.spark.{SparkConf, SparkContext}
import util.TestSuiteBuilder

import scala.collection.mutable.ListBuffer

/**
  * Created by rajanishivarajmaski1 on 4/3/17.
  */
class TestKMeanClustering extends TestSuiteBuilder {

  test("closestCentroid") {
    val vec1 = collection.mutable.Map[Int, Double]()
    vec1.put(0, 5.0)
    vec1.put(1, 1.0)
    vec1.put(2, 1.0)
    val vector1 = Vectors.sparse(5, vec1.toSeq)
    val vec2 = collection.mutable.Map[Int, Double]()
    vec1.put(0, 0.0)
    vec2.put(1, 0.0)
    vec2.put(2, 0.0)
    vec2.put(3, 1.0)
    val vector2 = Vectors.sparse(5, vec2.toSeq)

    val vec3 = collection.mutable.Map[Int, Double]()
    vec3.put(0, 5.0)
    vec3.put(1, 0.0)
    vec3.put(2, 0.0)
    val vector3 = Vectors.sparse(5, vec3.toSeq)

    val vec4 = collection.mutable.Map[Int, Double]()
    vec4.put(0, 5.0)
    vec4.put(1, 1.0)
    vec4.put(2, 0.0)
    val vector4 = Vectors.sparse(5, vec4.toSeq)

    val vecSec = List(vector1, vector2, vector3)

    println(vector1)
    println("center selected and similarity score", new KMeanClustering().closestCentroid(vecSec, vector1))

    println(vector2)
    println("center selected and similarity score", new KMeanClustering().closestCentroid(vecSec, vector2))

    println(vector3)
    println("center selected and similarity score", new KMeanClustering().closestCentroid(vecSec, vector3))

    println(vector4)
    println("center selected and similarity score", new KMeanClustering().closestCentroid(vecSec, vector4))

  }


  test("euclid") {
    val v1 = Vectors.sparse(4, Seq((0, 2.0), (1, 3.0), (2,4.0), (3,2.0)))
    val v2 = Vectors.sparse(4, Seq((0, 1.0), (1, -2.0), (2, 1.0), (3,3.0)))

   val  sqDist = Vectors.sqdist(v1, v2)

    println(math.sqrt(sqDist)) //6.0

    val v3 = Vectors.sparse(4, Seq((0, 5.0), (1, 4.8), (2,7.5), (3,10.0)))
    val v4 = Vectors.sparse(4, Seq((0, 3.9), (1, 4.1), (2, 6.2), (3,7.3)))

    val  sqDist2 = Vectors.sqdist(v3, v4)

    println(math.sqrt(sqDist2)) //3.2680269276736382

  }



  test("avgVectorValues"){
    val vector1 = Vectors.sparse(5, Seq((0, 1.0), (1, 1.0)))
    println("avgVectorValues\n", vector1 + "\n", 2.0 + "\n", new KMeanClustering().avgVectorValues(vector1, 2.0))
  }


  test("sumVectorValues"){
    val vector1 = Vectors.sparse(5, Seq((0, 1.0), (1, 1.0)))
    val vector2 = Vectors.sparse(5, Seq((0, 1.0), (1, 1.0), (2, 1.0)))

    println("sumVectorValues\n", vector1 + "\n", vector2 + "\n", new KMeanClustering().sumVectorValues(vector1, vector2))

  }


  test("getRandomNumberInRange"){
    val array = new KMeanClustering().setK(3).getRandomNumberInRange(0,6)
    println("random numbers chosen ")
    array.foreach(println(_))

  }
  test("initRandom") {
    val vector1 = Vectors.sparse(5, Seq((0, 1.0), (1, 1.0)))
    val vector2 = Vectors.sparse(5, Seq((0, 2.0), (1, 2.0), (2, 2.0)))
    val vector3 = Vectors.sparse(5, Seq( (1, 3.0), (3, 3.0),(4, 3.0)))
    val vector4 = Vectors.sparse(5, Seq( (0, 4.0), (1, 4.0), (3, 4.0), (4, 1.0)))
    val vector5 = Vectors.sparse(5, Seq( (0, 5.0), (1, 5.0), (2, 5.0), (3, 5.0)))

    val vector6 = Vectors.sparse(5, Seq( (0, 6.0), (1, 6.0), (3, 6.0), (4, 6.0)))

    val input = Seq(vector1, vector2, vector3,vector4,vector5, vector6)
    val array = new KMeanClustering().setK(2).initRandom(input.toArray)
    println("random vectors selected")
    array.foreach(println(_))

  }

  test("initRandomNonZeroVectors"){

    val vector1 = Vectors.sparse(5, Seq((0, 1.0), (1, 1.0)))
    val vector2 = Vectors.sparse(5, Seq((0, 2.0), (1, 2.0), (2, 2.0)))
    val vector3 = Vectors.sparse(5, Seq( (1, 3.0), (3, 3.0),(4, 3.0)))
    val vector4 = Vectors.sparse(5, Seq( (0, 4.0), (1, 4.0), (3, 4.0), (4, 1.0)))
    val vector5 = Vectors.sparse(5, Seq((0,0.0),(1,0.0)))

    val vector6 = Vectors.sparse(5, Seq( (0, 6.0), (1, 6.0), (3, 6.0), (4, 6.0)))
    val vector7 = Vectors.sparse(5, Seq((0,0.0)))

    val input = Seq(vector1, vector2, vector3,vector4,vector5, vector6, vector7)
    val array = new KMeanClustering().setK(4).initRandomNonZeroVectors(input.toArray)
    println("random vectors selected")
    array.foreach(println(_))

  }

  test("initVBR") {
    val vector1 = Vectors.sparse(5, Seq((0, 1.0), (1, 1.0)))
    val vector2 = Vectors.sparse(5, Seq((0, 2.0), (1, 2.0), (2, 2.0)))
    val vector3 = Vectors.sparse(5, Seq( (1, 3.0), (3, 3.0),(4, 3.0)))
    val vector4 = Vectors.sparse(5, Seq( (0, 4.0), (1, 4.0), (3, 4.0), (4, 1.0)))
    val vector5 = Vectors.sparse(5, Seq( (0, 5.0), (1, 5.0), (2, 5.0), (3, 5.0)))

    val vector6 = Vectors.sparse(5, Seq( (0, 6.0), (1, 6.0), (3, 6.0), (4, 6.0)))

    val input = Seq(vector1, vector2, vector3,vector4,vector5, vector6)

    val mean = (vector1.numNonzeros + vector2.numNonzeros + vector3.numNonzeros + vector4.numNonzeros +
      vector5.numNonzeros + vector6.numNonzeros)/6.0


    val dev = input.map(score => (score.numNonzeros - mean) * (score.numNonzeros - mean))
    val stdDev = Math.sqrt(dev.sum / (6.0 - 1))

  println(mean, stdDev)

    val array = new KMeanClustering().setK(2).initVBR(input.toArray, new VRS(average = mean,stdDev = stdDev))
    println("random vectors selected")
    array.foreach(println(_))

  }

  test("runKmean"){
    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("test app"))

    //0,1
    val vector1 = Vectors.sparse(5, Seq((0, 1.0), (1, 1.0)))

  //  0,1,2
   val vector2 = Vectors.sparse(5, Seq((0, 1.0), (1, 1.0), (2, 1.0)))

    //1,3,4
    val vector3 = Vectors.sparse(5, Seq( (1, 1.0), (3, 1.0),(4, 1.0)))

    //0,1,3
    val vector4 = Vectors.sparse(5, Seq( (0, 1.0),  (1, 1.0), (3, 1.0)))

    //0,1,2,3,4
    val vector5 = Vectors.sparse(5, Seq( (0, 1.0), (1, 1.0), (2, 1.0), (3, 1.0), (4, 1.0)))

    val input = Seq(vector1, vector2, vector3,vector4, vector5)
    val result = KMeanClustering.train(data = input.toArray,k=2,maxIterations = 10, numFeatures = 5)

    //printing final centroids.
    println("final centroids")
    result.foreach(res => println(res.toSparse))

    //printing results
     println("printing final results")
    input.foreach(vec => println(vec + "\n", KMeanClustering.predict(result,vec)._1))

  }


  test("runKmean_initVBR"){
    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("test app"))

    //0,1
    val vector1 = Vectors.sparse(5, Seq((0, 1.0), (1, 1.0)))

    //  0,1,2
    val vector2 = Vectors.sparse(5, Seq((0, 1.0), (1, 1.0), (2, 1.0)))

    //1,3,4
    val vector3 = Vectors.sparse(5, Seq( (1, 1.0), (3, 1.0),(4, 1.0)))

    //0,1,3
    val vector4 = Vectors.sparse(5, Seq( (0, 1.0),  (3, 1.0), (4, 1.0)))

    //0,1,2,3,4
    val vector5 = Vectors.sparse(5, Seq( (0, 1.0), (1, 1.0), (2, 1.0), (3, 1.0), (4, 1.0)))

    val input = Seq(vector1, vector2, vector3,vector4, vector5)

    //obtaining the term frequencies of each vector.
    var totalTf: ListBuffer[Double] = new ListBuffer[Double]
    input.foreach(vec => totalTf+=vec.numNonzeros)

    // dividing by number of documents.
    val mean= totalTf.sum/5

    val dev = totalTf.map(eachTf => (eachTf - mean) * (eachTf - mean))
    val stdDev = Math.sqrt(dev.sum / (5.0 - 1))

    println(mean, stdDev)
    val result = KMeanClustering.train(data = input.toArray,k=2,maxIterations = 5, numFeatures = 5, new VRS(average = mean,stdDev = stdDev))

    //printing final centroids.
    println("final centroids")
    result.foreach(res => println(res.toSparse))

    //printing results
    println("printing final results")
    input.foreach(vec => println(vec + "\n", KMeanClustering.predict(result,vec)))


  }

  test("runKmean_LongDocs"){
    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("test app"))

    //0,1,2,3,4
    val vector1 = Vectors.sparse(10, Seq( (0, 1.0), (1, 1.0), (2, 1.0), (3, 1.0), (4, 1.0)))

    //  0,1,2
    val vector2 = Vectors.sparse(10, Seq((0, 1.0), (1, 1.0), (2, 1.0)))

    //1,3,4
    val vector3 = Vectors.sparse(10, Seq( (1, 1.0), (3, 1.0),(4, 1.0)))

    //0,1,4
    val vector4 = Vectors.sparse(10, Seq( (0, 1.0),  (1, 1.0), (4, 1.0)))

    val vector5 = Vectors.sparse(10, Seq( (0, 1.0), (1, 1.0), (2, 1.0), (3, 1.0), (4, 1.0), (5, 1.0), (6, 1.0), (7, 1.0), (8, 1.0), (9, 1.0)))



    //val input = sc.parallelize(Seq(vector1, vector2, vector3,vector5))
    val input = Seq(vector1, vector2, vector3,vector4, vector5)
    val result = KMeanClustering.train(data = input.toArray,k=2,maxIterations = 5, numFeatures = 10)

    //printing final centroids.
    println("final centroids")
    result.foreach(res => println(res.toSparse))
    //printing results

    println("printing final results")
    input.foreach(vec => println(vec, KMeanClustering.predict(result,vec)))


  }


  test("runKmeanParallel"){
    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("test app"))

    //0,1
    val vector1 = Vectors.sparse(5, Seq((0, 1.0), (1, 1.0)))

    //  0,1,2
    val vector2 = Vectors.sparse(5, Seq((0, 1.0), (1, 1.0), (2, 1.0)))

    //1,3,4
    val vector3 = Vectors.sparse(5, Seq( (1, 1.0), (3, 1.0),(4, 1.0)))

    //0,1,3
    val vector4 = Vectors.sparse(5, Seq( (0, 1.0),  (1, 1.0), (3, 1.0)))

    //0,1,2,3,4
    val vector5 = Vectors.sparse(5, Seq( (0, 1.0), (1, 1.0), (2, 1.0), (3, 1.0), (4, 1.0)))

    val input = Seq(vector1, vector2, vector3,vector4, vector5)
    val rdd = sc.parallelize(input)
    val result = KMeanClustering.train(rdd,k=2,maxIterations = 5, numFeatures = 5)

    //printing final centroids.
    println("final centroids")
    result.foreach(res => println(res.toSparse))

    //printing results
    println("printing final results")
    input.foreach(vec => println(vec + "\n", KMeanClustering.predict(result,vec)._1))

  }

  test("axpy"){

    val vector2 = Vectors.sparse(5, Seq( (0, 2.0), (1, 3.0), (2, 4.0), (3, 1.0), (4, 1.0)))


    val vector5 = Vectors.sparse(5, Seq( (0, 2.0), (1, 3.0), (2, 4.0), (3, 1.0), (4, 1.0)))

    axpy(1.0, vector2.toSparse, vector5.toDense)

    println(vector5)
  }
  /**
    * y += a * x
    */
  private def axpy(a: Double, x: SparseVector, y: DenseVector): Unit = {
    val xValues = x.values
    val xIndices = x.indices
    val yValues = y.values
    val nnz = xIndices.length

    if (a == 1.0) {
      var k = 0
      while (k < nnz) {
        yValues(xIndices(k)) += xValues(k)
        k += 1
      }
    } else {
      var k = 0
      while (k < nnz) {
        yValues(xIndices(k)) += a * xValues(k)
        k += 1
      }
    }
  }


}
