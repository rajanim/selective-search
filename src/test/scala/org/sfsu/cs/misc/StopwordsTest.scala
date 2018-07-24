package org.sfsu.cs.misc

import java.io.BufferedReader
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

/**
  * Created by rajanishivarajmaski1 on 7/8/18.
  */
class StopwordsTest {



  def compareStopWordsfileFileIO(s1: String, s2: String): Unit ={

    var reader : BufferedReader = null
    var stopWordSet1 = scala.collection.mutable.Set[String]()



  }


  def compareStopWordsFile(stopWordsFile1: String, stopWordsFile2: String) = {

    val stopWords1 = scala.io.Source.fromFile(stopWordsFile1).getLines().toSet
    val sortedList1 = stopWords1.toList.sortWith(_ > _).iterator

    val stopWords2 = scala.io.Source.fromFile(stopWordsFile2).getLines().toSet
    val sortedList2 = stopWords2.toList.sortWith(_ > _).iterator

    while (sortedList1.hasNext) {
      if (sortedList2.nonEmpty) {
        val s1 = sortedList1.next().trim
        val s2 = sortedList2.next().trim
        if (!s1.equals(s2)) {
          println(s1, s2)
        }else{
          println("equal", s1, s2)
        }
      }
    }


  }
}

object StopwordsTest{

  def main(args: Array[String]): Unit = {

    val stopWordsCompare = new StopwordsTest
    stopWordsCompare.compareStopWordsFile("/Users/rajanishivarajmaski1/University/csc895/selective-search/src/test/resources/stopwords.txt",
      "/Users/rajanishivarajmaski1/University/csc895/selective-search/20180708_145711_stopwordsReWriteToTest.txt")

  }
}
