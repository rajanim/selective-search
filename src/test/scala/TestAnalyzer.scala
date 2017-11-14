import org.sfsu.cs.preprocess.CustomAnalyzer
import util.TestSuiteBuilder

/**
  * Created by rajanishivarajmaski1 on 11/14/17.
  */
class TestAnalyzer extends TestSuiteBuilder{

  test("kstem"){

    CustomAnalyzer.initStem()
    val stemmer = CustomAnalyzer.getStemmer()
    println(stemmer.stem("concerned"))
    println(stemmer.stem("windows"))


  }
}
