package util

import org.sfsu.cs.utils.Utility

/**
  * Created by rajanishivarajmaski1 on 11/15/17.
  */
class TestUtility extends TestSuiteBuilder{

  test("getFilePath"){

    Utility.writeToFile("test", Utility.getFilePath())
  }
}
