package SimJoins.DataStructure

import scala.collection.mutable

/**
  * Prefix index
  *
  * @author Luca
  * @since 2018/10/01
  **/
class PrefixIndex() {
  /**
    * Main index
    **/
  private val index: mutable.Map[Long, List[PrefixEntry]] = new mutable.HashMap[Long, List[PrefixEntry]]


  /**
    * Add an element to the index in a specific block
    **/
  def addElement(blockId: Long, doc: PrefixEntry): Unit = {
    val opt = index.get(blockId)
    if (opt.isDefined) {
      /*val block = doc :: opt.get
      index.update(blockId, block.sorted)*/
      index.update(blockId, doc :: opt.get)
    }
    else {
      index.put(blockId, List(doc))
    }
  }


  /**
    * Check if a block exists in the index
    **/
  def exists(blockId: Long): Boolean = {
    index.contains(blockId)
  }

  /**
    * Get a block from the index
    **/
  def getBlock(blockId: Long): Option[List[PrefixEntry]] = {
    index.get(blockId)
  }
}
