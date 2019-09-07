package org.sfsu.cs.io.bbc

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}

/**
  * Created by rajanishivarajmaski1 on 8/26/19.
  */
object BBCDatasetToSingleDirectory {

  def main(args: Array[String]): Unit = {

    val bbcPath = "/Users/rajanishivarajmaski1/Downloads/bbc/"
    val singleDirPath = "/Users/rajanishivarajmaski1/Downloads/bbc_singleDirPath/"

    mergeToSingleDirectory(bbcPath, singleDirPath)

  }

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def mergeToSingleDirectory(bbcPath : String, singleDirPath : String) ={
    val dir = new File(bbcPath)
    if(dir.isDirectory){
      val subDirs = dir.listFiles().iterator
          while (subDirs.hasNext){
            val subDir = subDirs.next()
            val files = getListOfFiles(subDir.getAbsolutePath)
            files.foreach( file => moveRenameFile(file.getAbsolutePath, singleDirPath+ subDir.getName +"_"+file.getName))
          }
          }
        }

  def moveRenameFile(source: String, destination: String): Unit = {
    val path = Files.copy(
      Paths.get(source),
      Paths.get(destination),
      StandardCopyOption.REPLACE_EXISTING
    )
    // could return `path`
  }
}
