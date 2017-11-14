package org.sfsu.cs.document

/**
  * https://github.com/rjagerman/mammoth/blob/master/src/main/scala/ch/ethz/inf/da/mammoth/document/Document.scala
  * An abstract document that has a unique identifier which is a string
  *
  * @param id The unique identifier
  */
class Document(id: String) extends Serializable
