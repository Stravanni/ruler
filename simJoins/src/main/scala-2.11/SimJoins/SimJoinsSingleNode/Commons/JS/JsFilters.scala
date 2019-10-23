package SimJoins.SimJoinsSingleNode.Commons.JS

/**
  * Created by Luca on 27/03/2018.
  */
object JsFilters {
  /**
    * Implementa il position filter come da formule teoriche
    * */
  def positionFilter(lenDoc1: Int, lenDoc2: Int, posDoc1: Int, posDoc2: Int, o: Int, threshold: BigDecimal): Boolean = {
    val alpha = Math.ceil((threshold * (lenDoc1 + lenDoc2) / (1 + threshold)).toDouble).toInt
    (o + Math.min(lenDoc1 - posDoc1, lenDoc2 - posDoc2)) >= alpha
  }

  /**
    * Implementa il length filter
    * */
  def lengthFilter(lenDoc1: Int, lenDoc2 : Int, threshold : BigDecimal): Boolean ={
    Math.min(lenDoc1, lenDoc2) >= Math.max(lenDoc1, lenDoc2) * threshold
  }

  /**
    * Dato un array di token ritorna il prefisso in base alla soglia, implementato correttamente in base alle formule
    **/
  def getPrefix(tokens: Array[Int], threshold: Double, k: Int = 1): Array[Int] = {
    val len = tokens.length
    tokens.take(len - Math.ceil(len.toDouble * threshold).toInt + k)
  }

  /**
    * Data la lunghezza di un documento e una soglia ritorna la lunghezza del prefisso
    * */
  def getPrefixLength(docLen: Int, threshold: Double, k: Int = 1) : Int = {
    docLen - Math.ceil(docLen.toDouble * threshold).toInt + 1
  }

}
