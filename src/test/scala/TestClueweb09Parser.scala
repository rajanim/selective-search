import org.apache.spark.{SparkConf, SparkContext}
import org.sfsu.cs.io.clueweb09.Clueweb09Parser
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
    val tfDocs = Clueweb09Parser.getTFDocuments(sc, stringDocs, 4, "/Users/rajanishivarajmaski1/University/csc895/selective-search/src/test/resources/stopwords.txt" )
    println(tfDocs.take(1).mkString(" "))
  }


}
