package SimJoins.SimJoinsSingleNode.Commons.JS

import SimJoins.DataStructure.PrefixEntry

object CommonJsFunctions {

  /**
    * Data una stringa esegue la tokenizzazione
    **/
  def tokenize(document: String): Set[String] = {
    //val stopWords = StopWordsRemover.loadDefaultStopWords("english").map(_.toLowerCase)
    document.split("[\\W_]").map(_.trim.toLowerCase).filter(_.length > 0).toSet //.filter(x => !stopWords.contains(x)).toSet
  }

  /**
    * Dati due array di token ordinati ritorna il numero di token che hanno in comune
    **/
  def getCommonTokensNum(tokens1: Array[Long], tokens2: Array[Long]): Int = {
    var result: Int = 0
    var i: Int = 0
    var j: Int = 0
    while (i < tokens1.length && j < tokens2.length) {

      if (tokens1(i) < tokens2(j)) {
        i += 1
      }
      else if (tokens1(i) > tokens2(j)) {
        j += 1
      }
      else {
        result += 1
        i += 1
        j += 1
      }

    }
    result
  }

  /**
    * Dati due documenti ritorna il numero di token che hanno in comune nel prefisso
    **/
  def getCommonPrefixElements(tokenId: Long, d1: Array[Int], d2: Array[Int], threshold: Double): Int = {
    var i = JsFilters.getPrefixLength(d1.length, threshold) - 1
    var j = JsFilters.getPrefixLength(d2.length, threshold) - 1
    var common = 0
    while (i >= 0 && j >= 0 && common >= 0) {
      if (d1(i) > d2(j)) {
        i -= 1
      }
      else if (d1(i) < d2(j)) {
        j -= 1
      }
      else if (common == 0 && d1(i) != tokenId) {
        common = -1
      }
      else {
        common += 1
        i -= 1
        j -= 1
      }
    }
    common
  }

  def buildPrefixIndex(tokenizedDocSort: List[(Long, Array[Int])], threshold: Double): Map[Int, List[PrefixEntry]] = {
    tokenizedDocSort.flatMap { case (docId, tokens) =>
      val prefix = JsFilters.getPrefix(tokens, threshold)
      prefix.zipWithIndex.map { case (token, position) =>
        (token, PrefixEntry(docId, position, tokens.length))
      }
    }.groupBy(_._1).filter(_._2.length > 1).map(x => (x._1, x._2.map(_._2)))
  }

  def tokenizeAndSort(documents: List[(Long, String)]): List[(Long, Array[Int])] = {
    val tf = calcTF(documents)
    documents.map { case (docId, content) =>
      (docId, tokenize(content).map(tf(_)).toArray.sorted)
    }
  }

  def calcEntropy(values: List[(Long, Array[Int])]): Double = {
    val tokens = values.flatMap(_._2)
    val tokensNum = tokens.length.toDouble
    val tokensProbabilities = tokens.groupBy(x => x).map(x => x._2.length).map { tokenFrequency =>
      val p_i = tokenFrequency / tokensNum
      p_i * (math.log10(p_i) / math.log10(2.0d))
    }

    val entropy = -tokensProbabilities.sum
    entropy
  }

  def calcTF(documents: List[(Long, String)]): Map[String, Int] = {
    val tokens = documents.flatMap(x => tokenize(x._2))
    tokens.groupBy(x => x).map(g => (g._1, g._2.size)).toList.sortBy(_._2).zipWithIndex.map(x => (x._1._1, x._2)).toMap
  }
}
