package SimJoins.SimJoinsSingleNode.Commons.ED

import SimJoins.DataStructure.Qgram

object CommonEdFunctions {
  object commons {
    def fixPrefix: (Int, Int) = (-1, -1)
  }

  /**
    * Dati due elementi ne calcola l'edit distance
    **/
  def editDist[A](a: Iterable[A], b: Iterable[A]): Int = {
    ((0 to b.size).toList /: a) ((prev, x) =>
      (prev zip prev.tail zip b).scanLeft(prev.head + 1) {
        case (h, ((d, v), y)) => math.min(math.min(h + 1, v + 1), d + (if (x == y) 0 else 1))
      }) last
  }

  /**
    * Dati i documenti trasformati in q-grammi calcola la document frequency per ogni qgramma
    **/
  def getQgramsTf(docs: List[(Long, Array[(String, Int)])]): Map[String, Int] = {
    val allQgrams = docs.flatMap { case (docId, qgrams) =>
      qgrams.map { case (str, pos) =>
        str
      }
    }
    allQgrams.groupBy(x => x).map(x => (x._1, x._2.size))
  }

  /**
    * Data una stringa ne restituisce i qgrammi.
    * Il qgramma ha anche la posizione originale nel documento
    **/
  def getQgrams(str: String, qgramSize: Int): Array[(String, Int)] = {
    str.sliding(qgramSize).zipWithIndex.map(q => (q._1, q._2)).toArray
  }

  /**
    * Ordina i qgrammi all'interno del documento per la loro document frequency
    **/
  def getSortedQgrams(docs: List[(Long, Array[(String, Int)])]): List[(Long, Array[(Int, Int)])] = {
    val tf = getQgramsTf(docs)
    val tf2 = tf.toList.sortBy(_._2).zipWithIndex.map(x => (x._1._1, x._2)).toMap
    docs.map { case (docId, qgrams) =>
      val sortedQgrams = qgrams.map(q => (tf2(q._1), q._2)).sortBy(q => q)
      (docId, sortedQgrams)
    }
  }

  def getSortedQgrams2(docs: List[(Long, String, Array[(String, Int)])]): List[(Long, String, Array[(Int, Int)])] = {
    val tf = getQgramsTf(docs.map(x => (x._1, x._3)))
    val tf2 = tf.toList.sortBy(_._2).zipWithIndex.map(x => (x._1._1, x._2)).toMap
    docs.map { case (docId, str, qgrams) =>
      val sortedQgrams = qgrams.map(q => (tf2(q._1), q._2)).sortBy(q => q)
      (docId, str, sortedQgrams)
    }
  }

  def calcEntropy(values : List[(Long, String, Array[(Int, Int)])]) : Double = {
    val tokens = values.flatMap(_._3)
    val tokensNum = tokens.length.toDouble
    val tokensProbabilities = tokens.groupBy(x => x).map(x => x._2.length).map { tokenFrequency =>
      val p_i = tokenFrequency / tokensNum
      p_i * (math.log10(p_i) / math.log10(2.0d))
    }
    val entropy = -tokensProbabilities.sum
    entropy
  }

  /**
    * Dato l'elenco di documenti con i q-grammi ordinati crea il prefix index.
    * Nota: per risolvere il problema dei documenti troppo corti il prefix index contiene un blocco identificato dall'id
    * specificato in "fixprefix" che contiene tutti i documenti che non possono essere verificati con sicurezza.
    * */
  def buildPrefixIndex(sortedDocs: List[(Long, Array[(Int, Int)])], qgramLen: Int, threshold: Int): Map[Int, List[Qgram]] = {
    val prefixLen = EdFilters.getPrefixLen(qgramLen, threshold)

    val allQgrams = sortedDocs.flatMap { case (docId, qgrams) =>
      val prefix = {
        if (qgrams.length < prefixLen) {
          qgrams.union(commons.fixPrefix :: Nil)
        }
        else {
          qgrams.take(prefixLen)
        }
      }
      prefix.zipWithIndex.map { case (qgram, index) =>
        (qgram._1, Qgram(docId, qgrams.length, qgram._2, index))
      }
    }
    allQgrams.groupBy(_._1).filter(_._2.size > 1).map(x => (x._1, x._2.map(_._2)))
  }
}
