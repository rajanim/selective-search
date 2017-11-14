package org.sfsu.cs.preprocess


import org.jsoup.Jsoup
import org.lemurproject.kstem.KrovetzStemmer

import scala.collection.mutable

/**
  * Created by rajanishivarajmaski1 on 2/11/17.
  */
object CustomAnalyzer {

  var stem: KrovetzStemmer = null

  /**
    * Initialize stemmer if this filter is applied while analysing input string
    */
  def initStem() = stem = new KrovetzStemmer


  def getStemmer() : KrovetzStemmer ={
    stem
  }
  /**
    * Analyzer : Tokenizes and filters by lowercase, stopwords
    *
    * @param input
    * @return
    */
  def tokenizeFilterStopWords(input: String, stopWords: Set[String]): Map[Any, Double] = {
    val terms = input.toLowerCase.split("\\W+").filter(!stopWords(_)).iterator
    val termList = mutable.HashMap.empty[Any, Double]
    while (terms.hasNext) {
      val term = terms.next()
      if (!isAllDigits((term)))
        termList.put(term, tf(term, termList))
    }
    termList.toMap
  }


  /**
    * Analyzer : Tokenizes and filters by lowercase, stopwords and applies KStem, filters terms containing only digits.
    *
    * @param input
    * @return
    */
  def tokenizeFilterStopWordsStem(input: String, stopWords: Set[String]): Map[Any, Double] = {
    val terms = input.toLowerCase.split("\\W+").filter(!stopWords(_)).iterator
    val termList = mutable.HashMap.empty[Any, Double]
    while (terms.hasNext) {
      val term = terms.next()
      val stemTerm = stem.stem(term)
      if (!isAllDigits((stemTerm)))
        termList.put(stemTerm, tf(stemTerm, termList))
    }
    termList.toMap
  }


  def tf(term: Any, tfMap: mutable.Map[Any, Double]): Double = {
    if (tfMap.contains(term))
      tfMap(term) + 1
    else
      1.0
  }


  /**
    * regex to check if input token has only digits
    *
    * @param x
    * @return
    */
  def isAllDigits(x: String) = x.matches("^\\d*$")


  /**
    * Processes an input stream and extracts the plain text*
    *
    * @param input The input html
    * @return The plain text representation
    */
  def htmlToText(input: String): String = try {
    Jsoup.parse(input).body().text()
  } catch {
    case e: Exception => ""
    case e: StackOverflowError => ""
  }

}
