package org.sfsu.cs.utils

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by rajanishivarajmaski1 on 11/14/17.
  */
object Utility {

  def writeToFile(data: String, file: String) = {
    println("writeToFile", file)
    val output = new FileOutputStream(file)
    output.write(data.getBytes())
    output.close()

  }

  def getFilePath(): String = {
    val path = System.getProperty("user.dir") + "/spark_cluster_job_op/"
    path + new SimpleDateFormat("yyyyMMdd'_'HHmmss").format(new Date)
  }
}
