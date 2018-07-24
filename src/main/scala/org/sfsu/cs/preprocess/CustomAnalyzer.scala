package org.sfsu.cs.preprocess


import org.jsoup.Jsoup
import org.lemurproject.kstem.KrovetzStemmer
import org.sfsu.cs.utils.Utility

import scala.collection.mutable

/**
  * Created by rajanishivarajmaski1 on 2/11/17.
  */
object CustomAnalyzer {

  var stem: KrovetzStemmer = new KrovetzStemmer

  /**
    * Initialize stemmer if this filter is applied while analysing input string
    */
  def initStem() = stem = new KrovetzStemmer


  def getStemmer(): KrovetzStemmer = {
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
    val ioTerms = input.toLowerCase.split("[0-9]+|[\\W]").filterNot(stopWords)
   // Utility.appendResultsToFile(ioTerms.mkString(",") + "\n", "/Users/rajanishivarajmaski1/University/CSC899/spark-2.0.0-bin-hadoop2.7/spark_cluster_job_op/terms.txt")
   // val ioTerms = input.toLowerCase.split("[0-9]+|[\\W]")
    //val stopWordsFiltered = ioTerms.filterNot(stopWords)
    //stopWordsFiltered.foreach(println(_))
    val terms = ioTerms.iterator
    val termList = mutable.HashMap.empty[Any, Double]
    while (terms.hasNext) {
      val term = terms.next()
      if (term.size > 2) {
        val stemTerm = stem.stem(term)
        if(!stopWords.contains(stemTerm))
        if (!isAllDigits((stemTerm)))
          termList.put(stemTerm, tf(stemTerm, termList))
      }
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
  def isAllDigits(x: String) = {
    if(x != null)
    x.matches("^\\d*$")
    else
      true
  }


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

  def getStopWords() = stopWords

  /**
    * Stopwords that should be removed
    *
    * Collected from:
    * http://www.textfixer.com/resources/common-english-words.txt
    * http://www.ranks.nl/stopwords/ (all but the last block)
    */
  val stopWords = Set("a", "a's", "able", "about", "above", "according", "accordingly", "across", "actually", "after",
    "afterwards", "again", "against", "ain't", "all", "allow", "allows", "almost", "alone", "along", "already", "also",
    "although", "always", "am", "among", "amongst", "an", "and", "another", "any", "anybody", "anyhow", "anyone",
    "anything", "anyway", "anyways", "anywhere", "apart", "appear", "appreciate", "appropriate", "are", "aren't",
    "around", "as", "aside", "ask", "asking", "associated", "at", "available", "away", "awfully", "be", "became",
    "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "believe", "below",
    "beside", "besides", "best", "better", "between", "beyond", "both", "brief", "but", "by", "c'mon", "c's", "came",
    "can", "can't", "cannot", "cant", "cause", "causes", "certain", "certainly", "changes", "clearly", "co", "com",
    "come", "comes", "concerning", "consequently", "consider", "considering", "contain", "containing", "contains",
    "corresponding", "could", "couldn't", "course", "currently", "dear", "definitely", "described", "despite", "did",
    "didn't", "different", "do", "does", "doesn't", "doing", "don't", "done", "down", "downwards", "during", "each",
    "edu", "eg", "eight", "either", "else", "elsewhere", "enough", "entirely", "especially", "et", "etc", "even",
    "ever", "every", "everybody", "everyone", "everything", "everywhere", "ex", "exactly", "example", "except", "far",
    "few", "fifth", "first", "five", "followed", "following", "follows", "for", "former", "formerly", "forth", "four",
    "from", "further", "furthermore", "get", "gets", "getting", "given", "gives", "go", "goes", "going", "gone", "got",
    "gotten", "greetings", "had", "hadn't", "happens", "hardly", "has", "hasn't", "have", "haven't", "having", "he",
    "he'd", "he'll", "he's", "hello", "help", "hence", "her", "here", "here's", "hereafter", "hereby", "herein",
    "hereupon", "hers", "herself", "hi", "him", "himself", "his", "hither", "hopefully", "how", "how's", "howbeit",
    "however", "i", "i'd", "i'll", "i'm", "i've", "ie", "if", "ignored", "immediate", "in", "inasmuch", "inc", "indeed",
    "indicate", "indicated", "indicates", "inner", "insofar", "instead", "into", "inward", "is", "isn't", "it", "it'd",
    "it'll", "it's", "its", "itself", "just", "keep", "keeps", "kept", "know", "known", "knows", "last", "lately",
    "later", "latter", "latterly", "least", "less", "lest", "let", "let's", "like", "liked", "likely", "little", "look",
    "looking", "looks", "ltd", "mainly", "many", "may", "maybe", "me", "mean", "meanwhile", "merely", "might", "more",
    "moreover", "most", "mostly", "much", "must", "mustn't", "my", "myself", "name", "namely", "nd", "near", "nearly",
    "necessary", "need", "needs", "neither", "never", "nevertheless", "new", "next", "nine", "no", "nobody", "non",
    "none", "noone", "nor", "normally", "not", "nothing", "novel", "now", "nowhere", "obviously", "of", "off", "often",
    "oh", "ok", "okay", "old", "on", "once", "one", "ones", "only", "onto", "or", "other", "others", "otherwise",
    "ought", "our", "ours", "ourselves", "out", "outside", "over", "overall", "own", "particular", "particularly",
    "per", "perhaps", "placed", "please", "plus", "possible", "presumably", "probably", "provides", "que", "quite",
    "qv", "rather", "rd", "re", "really", "reasonably", "regarding", "regardless", "regards", "relatively",
    "respectively", "right", "said", "same", "saw", "say", "saying", "says", "second", "secondly", "see", "seeing",
    "seem", "seemed", "seeming", "seems", "seen", "self", "selves", "sensible", "sent", "serious", "seriously", "seven",
    "several", "shall", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "since", "six", "so",
    "some", "somebody", "somehow", "someone", "something", "sometime", "sometimes", "somewhat", "somewhere", "soon",
    "sorry", "specified", "specify", "specifying", "still", "sub", "such", "sup", "sure", "t's", "take", "taken",
    "tell", "tends", "th", "than", "thank", "thanks", "thanx", "that", "that's", "thats", "the", "their", "theirs",
    "them", "themselves", "then", "thence", "there", "there's", "thereafter", "thereby", "therefore", "therein",
    "theres", "thereupon", "these", "they", "they'd", "they'll", "they're", "they've", "think", "third", "this",
    "thorough", "thoroughly", "those", "though", "three", "through", "throughout", "thru", "thus", "tis", "to",
    "together", "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying", "twas", "twice", "two",
    "un", "under", "unfortunately", "unless", "unlikely", "until", "unto", "up", "upon", "us", "use", "used", "useful",
    "uses", "using", "usually", "value", "various", "very", "via", "viz", "vs", "want", "wants", "was", "wasn't", "way",
    "we", "we'd", "we'll", "we're", "we've", "welcome", "well", "went", "were", "weren't", "what", "what's", "whatever",
    "when", "when's", "whence", "whenever", "where", "where's", "whereafter", "whereas", "whereby", "wherein",
    "whereupon", "wherever", "whether", "which", "while", "whither", "who", "who's", "whoever", "whole", "whom",
    "whose", "why", "why's", "will", "willing", "wish", "with", "within", "without", "won't", "wonder", "would",
    "wouldn't", "www", "yes", "yet", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself",
    "yourselves", "zero", "subject", "organization", "lines", "zero", "clueweb09","clueweb012", "indri", "metadata", "html","type",
    "will", "doc", "document", "dd", "del", "delete", "inverted", "invert", "index", "dumpindex", "term", "vocabulary", "external",
    "command", "compact", "fieldposition", "termposition", "documentdata", "documentid", "documentname", "documenttext", "documentvector",
    "ids", "vector", "tp", "argument", "gmt", "change", "di", "dt", "dn", "dv", "dx", "dxcount", "fp", "invlist", "stat", "statistics",
    "expressionlist", "dd", "occurrence", "position", "merge", "match", "release", "repository", "retrieve", "print", "representation", "full"
  )

}
