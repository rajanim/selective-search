import org.apache.spark.{SparkConf, SparkContext}
import org.sfsu.cs.io.text.TextFileParser
import util.TestSuiteBuilder

/**
  * Created by rajanishivarajmaski1 on 11/14/17.
  */
class TestTextFileParser extends TestSuiteBuilder {

  test("getTfDocuments") {
    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("test app"))
    val tfDocs = TextFileParser.getTFDocuments(sc,
      "/Users/rajanishivarajmaski1/University/csc895/selective-search/src/test/resources/test_records",
      2, "/Users/rajanishivarajmaski1/University/csc895/selective-search/src/test/resources/stopwords.txt")
    println(tfDocs.take(1).mkString(" "))

  }
}
