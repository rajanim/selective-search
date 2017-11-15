import org.apache.spark.{SparkConf, SparkContext}
import org.sfsu.cs.io.clueweb09.Clueweb09Parser
import org.sfsu.cs.vectorize.VectorImpl
import util.TestSuiteBuilder

/**
  * Created by rajanishivarajmaski1 on 11/13/17.
  */
class TestClueweb09Parser extends TestSuiteBuilder {


  test("getWarcRecords") {
    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("test app"))
    val stringDocs = Clueweb09Parser.getWarcRecordsViaFIS(sc, "/Users/rajanishivarajmaski1/ProjectsToTest/clueweb_20/", 4)
    println("stringDocs count", stringDocs.count)
    assertResult(37237)(stringDocs.count())
  }

  test("getTFDocuments") {
    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("test app"))
    val stringDocs = Clueweb09Parser.getWarcRecordsViaFIS(sc, "/Users/rajanishivarajmaski1/ProjectsToTest/clueweb_20/", 4)
    println("stringDocs count", stringDocs.count)
    assertResult(37237)(stringDocs.count())
    val tfDocs = Clueweb09Parser.getTFDocuments(sc, stringDocs, 4, "" )
    println(tfDocs.take(1).mkString(" "))
  }

  test("getDocVectors"){
    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("test app"))
    val stringDocs = Clueweb09Parser.getWarcRecordsViaFIS(sc, "/Users/rajanishivarajmaski1/ProjectsToTest/clueweb_20/", 4)
    println("stringDocs count", stringDocs.count)
    assertResult(37237)(stringDocs.count())
    val tfDocs = Clueweb09Parser.getTFDocuments(sc, stringDocs, 4, "" )
    println(tfDocs.take(1).mkString(" "))
    val docVectors = VectorImpl.getDocVectors(sc,tfDocs,20)
    println(docVectors.take(1).mkString(" "))

  }


}
