package org.sfsu.cs.document

/**
  * A document whose contents is stored as a list of tokens
  *
  * @param id The unique identifier

  */
case class DocVector(id: String, tfMap : Map[Any, Double], vector : org.apache.spark.mllib.linalg.Vector) extends Document(id)
