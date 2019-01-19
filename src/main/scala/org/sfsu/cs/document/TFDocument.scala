package org.sfsu.cs.document


/**
  * A document whose contents is stored as a list of tokens
  *
  * @param id The unique identifier

  */
case class TFDocument(id: String, tfMap : Map[Any, Double], contentText: String) extends Document(id)
