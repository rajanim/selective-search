package org.sfsu.cs.clustering.kmeans

import java.util.Random

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.sfsu.cs.clustering.kmeans.distance.{CosineDistanceMeasure, EuclideanDistanceMeasure}
import org.sfsu.cs.utils.Utility

import scala.collection.mutable


/**
  * Created by rajanishivarajmaski1 on 3/29/17.
  */
class KMeanClustering private(
                               private var k: Int,
                               private var maxIterations: Int, private var numFeatures: Int,
                               private val normalizer: org.apache.spark.mllib.feature.Normalizer
                             ) extends Serializable {

  var centroids: Array[Vector] = Array.fill(k)(Vectors.dense(Array.fill(numFeatures)(0.0)))

  //VRS : Vocabulary Based Rejection Sampling range for selecting seeds
  var vrs: VRS = new VRS(0, 0)

  def setVRS(vbr: VRS): this.type = {
    this.vrs = vbr
    this
  }

  def getVRS: VRS = vrs

  def this() = this(2, 20, 50, new org.apache.spark.mllib.feature.Normalizer())


  def getK: Int = k

  def setK(k: Int): this.type = {
    require(k > 0,
      s"Number of clusters must be positive but got ${k}")
    this.k = k
    this
  }


  def setClusterCenters(centroids: Array[Vector]): this.type = {
    this.centroids = centroids
    this
  }

  def getClusterCenters(): Array[Vector] = centroids

  def getNumFeatures: Int = numFeatures

  def setNumFeatures(numFeatures: Int): this.type = {
    require(numFeatures > 0,
      s"Number of features must be positive but got ${numFeatures}")
    this.numFeatures = numFeatures
    this
  }

  def getMaxIterations: Int = maxIterations

  def setMaxIterations(maxIterations: Int): this.type = {
    require(maxIterations >= 0,
      s"Maximum of iterations must be nonnegative but got ${maxIterations}")
    this.maxIterations = maxIterations
    this
  }

  def transformVectors(vector: Vector): Vector = {
    normalizer.transform(vector)
  }

  def transformVectors(vectors: RDD[Vector]): RDD[Vector] = {
    normalizer.transform(vectors)
  }


  /**
    * run algorithm
    *
    * @param vectors
    * @return
    */
  private def runKMean(vectors: Array[Vector]): Array[Vector] = {
    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("KMeanClustering"))

    //normalize vectors with norm
    val normalizedVectors = for (e <- vectors) yield transformVectors(e)


    //seed selection
    println(s" vbr average ${vrs.getAverage} and vbr std deviation ${vrs.getStdDev}")

    val seeds = if (vrs.getAverage != 0) {
      initVBR(normalizedVectors, vrs)
    }
    else {
      // initRandom(normalizedVectors)
      initRandomNonZeroVectors(normalizedVectors)
    }


    println("writing seeds to file location", Utility.getFilePath())
    Utility.writeToFile(seeds.mkString("\n"), Utility.getFilePath() + "_seeds")

    //run kmean until result centroids are obtained
    kmeans(normalizedVectors, seeds, sc)

  }


  /**
    * using spark apis to run kmean over rdd of vectors
    * @param vectors
    * @return
    */
  private def runKMeanParallel(vectors: RDD[Vector]): Array[Vector] = {
    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("KMeanClustering"))

    val normalizedVectors = transformVectors(vectors)

    //seed selection
    println(s" vbr average ${vrs.getAverage} and vbr std deviation ${vrs.getStdDev}")

    /*val seeds =
      initRandomNonZeroVectors(normalizedVectors)
*/
    val seeds = initRandomSampling(normalizedVectors)

    println("writing seeds to file location", Utility.getFilePath())
    Utility.writeToFile(seeds.mkString("\n"), Utility.getFilePath() + "_seeds")

    parallelKmeans(normalizedVectors, seeds, sc)

  }


  /**
    * get random seeds centroids
    *
    * @param data
    * @return
    */
  private def initRandom(data: RDD[Vector]): Array[Vector] = {
    data.takeSample(true, k, Math.random().toLong)
  }

  /**
    * Obtain seeds based on random number selected in range of array of vectors passed as input data.
    * @param data
    * @return
    */
  def initRandom(data: Array[Vector]): Array[Vector] = {
    val array: Array[Vector] = new Array(k)
    val randNums = getRandomNumberInRange(0, data.length - 1)
    println("Random seeds chosen: " + randNums.mkString(" "))

    var i = -1
    for (e <- randNums) yield {
      i += 1
      array(i) = data(e)
    }

    array
  }


  /**
    * Obtain uniformly random selected seeds
    * @param rddData
    * @return
    */
  def initRandomSampling(rddData : RDD[Vector]) : Array[Vector] = {
    val rand = new Random
    val seed = rand.nextLong()
    val nonZeroVectorsRDD = rddData.filter(v => v.numNonzeros>2)
    nonZeroVectorsRDD.takeSample(true, k, seed)
  }

  /**
    *From RDD of vectors, obtain only those vectors as seeds which have vector values (non zero vectors)
    * @param rddData
    * @return
    */
  def initRandomNonZeroVectors(rddData: RDD[Vector]): Array[Vector] = {
    val data = rddData.collect()
    val array: Array[Vector] = new Array(k)
    var i = 0
    val r = new Random
    val min = 0
    val max = data.length - 1
    val ints = scala.collection.mutable.Set[Int]()
    //var loopTimes = 0
    while (i < k) {
      //loopTimes+=1
      val index = getIndex(ints, r, min, max)
      ints += index
      val nonZeros = data(index).numNonzeros
      if (nonZeros >= 1) {
        array(i) = data(index)
        i += 1

      }
      //todo : need to write some logic to stop infinite looping
      /* if(loopTimes>max && data){
         throw new Exception("Either data does not have non zero vectors or k is set to higher value than vectors size")
       }*/
    }

    println("Random seeds chosen: " + ints.mkString(" "))

    array
  }

  /**
    * Obtain only those vectors as seeds which have vector values (non zero vectors)
    * @param data
    * @return
    */
  def initRandomNonZeroVectors(data: Array[Vector]): Array[Vector] = {
    val array: Array[Vector] = new Array(k)
    var i = 0
    val r = new Random
    val min = 0
    val max = data.length - 1
    val ints = scala.collection.mutable.Set[Int]()
    //var loopTimes = 0
    while (i < k) {
      //loopTimes+=1
      val index = getIndex(ints, r, min, max)
      ints += index
      val nonZeros = data(index).numNonzeros
      if (nonZeros >= 1) {
        array(i) = data(index)
        i += 1

      }
      //todo : need to write some logic to stop infinite looping
      /* if(loopTimes>max && data){
         throw new Exception("Either data does not have non zero vectors or k is set to higher value than vectors size")
       }*/
    }

    println("Random seeds chosen: " + ints.mkString(" "))

    array
  }



  def getIndex(ints: mutable.Set[Int], r: Random, min: Int, max: Int): Int = {
    val index = r.nextInt((max - min) + 1) + min
    if (ints.contains(index)) {
      getIndex(ints, r, min, max)
    } else return index
  }


  /**
    * Obtain seeds applying vocabulary based rejection sampling range.
    * @param data
    * @param vbr
    * @return
    */
  def initVBR(data: Array[Vector], vbr: VRS): Array[Vector] = {
    val array: Array[Vector] = new Array(k)
    var i = 0
    val r = new Random
    val min = 0
    val max = data.length - 1
    val minAvg = math.abs(vbr.getAverage - vbr.getStdDev).toInt
    val maxAvg = math.abs(vbr.getAverage + vbr.getStdDev).toInt
    val ints = scala.collection.mutable.Set[Int]()
    println(s"min terms count ${minAvg}  and max terms count ${maxAvg} and k is $k")
    while (i < k) {
      val index = getIndex(ints, r, min, max)
      ints += index
      val nonZeros = data(index).numNonzeros
      //todo: need to check for the case when there are less than k number of docs meeting this condition?
      //test_records case.
      if (nonZeros >= minAvg && nonZeros <= maxAvg) {
        // if (nonZeros >= math.abs(vbr.getAverage).round) {
        array(i) = data(index)
        i += 1

      }

    }
    array
  }


  /**
    * Sums 2 vectors values at respective positions and returns the summed third vector
    * @param vector1
    * @param vector2
    * @return
    */
  //todo : at this point sparse are converted to dense and so there can be possibility of array index out of bound.
  //need to handle the exception.
  def sumVectorValues(vector1: Vector, vector2: Vector): Vector = {
    val v1Array = vector1.toArray
    val v2Array = vector2.toArray
    for (i <- 0 until v1Array.length) {
      v1Array(i) += v2Array(i)
    }

    Vectors.dense(v1Array)
  }

  /**
    *Averages vector values to given number.
    * Here in this case, vectors belonging to same clusters are summed up  and values are averaged out by cluster count
    * @param vector
    * @param i
    * @return
    */
  def avgVectorValues(vector: Vector, i: Double): Vector = {
    val avgValues = vector.toArray.map(_ / i)
    Vectors.dense(avgValues)
  }

  /**
    * Sequential Kmean, uses straight forward scala/java apis to implement kmean algorithms.
    * RDD are converted to array of vectors and kmean is executed on array of vectors.
    * @param vectors
    * @param seeds
    * @param sc
    * @return
    */
  def kmeans(vectors: Array[Vector], seeds: Array[Vector], sc: SparkContext): Array[Vector] = {
    var iterations = 0
    var centers: Array[Vector] = Array.fill(k)(Vectors.dense(Array.fill(numFeatures)(0.0)))
    centers = seeds
    while (iterations < maxIterations) {
      //   current centroids
      // centers.foreach(println(_))
      //index array, size of k, that sums(accumulates) vectors values of vectors belonging to same cluster
      val summingCentroids: Array[Vector] = Array.fill(k)(Vectors.dense(Array.fill(numFeatures)(0.0)))
      // println("new and empty summing centroids")
      //summingCentroids.foreach(println(_))

      //index array size of k, counter(accumulates) to count number of vectors belonging to same cluster.
      val clusterCounts: Array[Int] = Array.fill(k)(0)
      // println("empty clusterCounts array")
      //clusterCounts.foreach(println(_))

      // looping for each vector to match its closest centroid index
      vectors.foreach(vector => {
        // println(" processing vector", vector)
        //val (index: Int, score: Double) = euclideanBasedClosestCentroid(centers, vector)
        val (index: Int, score: Double) = closestCentroid(centers, vector)

        //println("index selected: ", index)
        summingCentroids(index) = sumVectorValues(summingCentroids(index), vector)
        // println("accumulating summing centroids")
        //  summingCentroids.foreach(println(_))

        clusterCounts(index) += 1
        //  println("clusterCount accum")
        // clusterCounts.foreach(println(_))

      })

      //println("accumulated summingCentroids")
      //summingCentroids.foreach(println(_))
      //println("accumulated clusterCounts")
      //clusterCounts.foreach(println(_))

      //println("averaging summingCentroids by dividing each centroids' vector array with cluster count")
      //averaging vectors by dividing vectors value with cluster count.
      for (k <- 0 until summingCentroids.length) {
        summingCentroids(k) = avgVectorValues(summingCentroids(k), clusterCounts(k))

      }
      val normalizedCentroids = for (e <- summingCentroids) yield transformVectors(e)

      iterations += 1
      centers = normalizedCentroids
      // println(" printing new set of centroids")
      //centers.foreach(println(_))

    }
    centers

  }

  /**
    * kmean algorithm that computes summed centroid for each partition vectors and finally merges these summed centroids to obtain
    * average centroid at each iteration.
    * @param data
    * @param seeds
    * @param sc
    * @return
    */
  def parallelKmeans(data: RDD[Vector], seeds: Array[Vector], sc: SparkContext): Array[Vector] = {
    type WeightedPoint = (Vector, Int)

    def mergeContribs(x: WeightedPoint, y: WeightedPoint): WeightedPoint = {
      val vec = sumVectorValues(x._1, y._1)
      (vec, x._2 + y._2)
    }

    var centers: Array[Vector] = Array.fill(k)(Vectors.dense(Array.fill(numFeatures)(0.0)))
    centers = seeds
    var iterations = 0
    while (iterations < maxIterations) {
      //broadcast current iteration centers
      val bcActiveCenters = sc.broadcast(centers)

      val totalContribs = data.mapPartitions { points =>
        //each partition, map its vector to one of the center,
        // update summing vector //cluster count
        val thisActiveCenters = bcActiveCenters.value

       // println("thisActiveCenters: ", thisActiveCenters.mkString("\n"))

        val summingVector: Array[Vector] = Array.fill(k)(Vectors.dense(Array.fill(numFeatures)(0.0)))
        val clusterCounts: Array[Int] = Array.fill(k)(0)
        points.foreach { vector =>
          val (index: Int, score: Double) = closestCentroid(thisActiveCenters, vector)
          summingVector(index) = sumVectorValues(summingVector(index), vector)
          clusterCounts(index) += 1
        }

        val contribs = for (j <- 0 until k) yield {
          ((j), (summingVector(j), clusterCounts(j)))
        }
        contribs.iterator
      }.reduceByKey(mergeContribs).collectAsMap()

      bcActiveCenters.unpersist(blocking = false)


      var j = 0
      while (j < k) {
        val (sum, count) = totalContribs((j))
        if (count != 0) {

          centers(j) = avgVectorValues(sum, count)
        }
        j += 1
      }


      val normalizedCentroids = for (e <- centers) yield transformVectors(e)

      iterations += 1
      centers = normalizedCentroids

    }
    centers

  }


  /**
    * Finds the closest centroid to the given point. Uses cosine Distance measure by default.
    */
  def closestCentroid(centers: Iterable[Vector], point: Vector): (Int, Double) = {
    var bestSim = 0.0;
    var bestIndex = 0
    var i = 0
    centers.foreach { center =>
      val sim: Double = cosineDistanceMeasure(center, point)
      if (sim > bestSim) {
        bestSim = sim
        bestIndex = i
      }
      i += 1
    }
    (bestIndex, bestSim)

  }

  /**
    * computes euclidean distance between 2 vector points.
    * @param centers
    * @param point
    * @return
    */
  def euclideanBasedClosestCentroid(centers: Iterable[Vector], point: Vector): (Int, Double) = {
    var bestDistance = Double.PositiveInfinity
    var bestIndex = 0
    var i = 0
    centers.foreach { center =>
      val distance: Double = EuclideanDistanceMeasure.distance(center, point)
      if(distance < bestDistance){
        bestDistance = distance
        bestIndex = i
      }
      i += 1
    }
    (bestIndex, bestDistance)
}


  /**
    * computes cosine distance between 2 vectors.
    * @param vector1
    * @param vector2
    * @return distance
    */
  def cosineDistanceMeasure(vector1: Vector, vector2: Vector): Double = {
    CosineDistanceMeasure.distance(vector1, vector2)
  }


  /**
    * utility method to get randon number in range to select seeds from given set of vectors.
    * @param min
    * @param max
    * @return
    */
  def getRandomNumberInRange(min: Int, max: Int): Array[Int] = {
    val r = new Random;
    val ints = scala.collection.mutable.Set[Int]()
    while (ints.size < k)
      ints += r.nextInt((max - min) + 1) + min
    ints.toArray

  }

}

object KMeanClustering {


  def train(
             data: Array[Vector],
             k: Int,
             maxIterations: Int, numFeatures: Int
           ): Array[Vector] = {

    val kmean = new KMeanClustering().setK(k)
      .setMaxIterations(maxIterations)
      .setNumFeatures(numFeatures)

    kmean.runKMean(data)

  }

  def train(
             data: Array[Vector],
             k: Int,
             maxIterations: Int, numFeatures: Int, vbr: VRS
           ): Array[Vector] = {

    val kmean = new KMeanClustering().setK(k)
      .setMaxIterations(maxIterations)
      .setNumFeatures(numFeatures)
      .setVRS(vbr)

    kmean.runKMean(data)

  }


  def train(
             data: RDD[Vector],
             k: Int,
             maxIterations: Int, numFeatures: Int
           ): Array[Vector] = {

    val kmean = new KMeanClustering().setK(k)
      .setMaxIterations(maxIterations)
      .setNumFeatures(numFeatures)

    kmean.runKMeanParallel(data)

  }

  def predict(centroids: Array[Vector], point: Vector): (Int, Double) = {

    new KMeanClustering().closestCentroid(centroids, point)
  }


}










