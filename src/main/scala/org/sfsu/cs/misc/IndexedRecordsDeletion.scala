package org.sfsu.cs.misc

import com.lucidworks.spark.util.SolrSupport

import scala.io.Source

/**
  * Created by rajanishivarajmaski1 on 4/24/18.
  */
class IndexedRecordsDeletion {




  def deleteRecordsById(zkHost: String, collection: String, fileName: String) = {

    val client = SolrSupport.getCachedCloudClient(zkHost)

    for (line <- Source.fromFile(fileName).getLines) {
      val id = " "+ line.trim + ".html"
      client.deleteById(collection, id)
    }

    client.commit(collection, true, false)
  }
}

object IndexedRecordsDeletion{

  def main(args: Array[String]): Unit = {
    val recordsDeletion = new IndexedRecordsDeletion
    recordsDeletion.deleteRecordsById("localhost:9983", "clueweb", "/Users/rajanishivarajmaski1/ClueWeb09_English_9/qrels_docs/fileNames.txt")
  }

}
