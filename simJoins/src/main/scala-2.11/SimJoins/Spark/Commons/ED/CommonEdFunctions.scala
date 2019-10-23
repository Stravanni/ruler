package SimJoins.Spark.Commons.ED

import SimJoins.DataStructure.Qgram
import SimJoins.SimJoinsSingleNode.Commons.ED.CommonEdFunctions.commons
import SimJoins.SimJoinsSingleNode.Commons.ED.EdFilters
import org.apache.spark.rdd.RDD

object CommonEdFunctions {
  /**
    * Dati i documenti trasformati in q-grammi calcola la document frequency per ogni qgramma
    **/
  def getQgramsTf(docs: RDD[(Long, Array[(String, Int)])]): Map[String, Int] = {
    val allQgrams = docs.flatMap { case (docId, qgrams) =>
      qgrams.map { case (str, pos) =>
        str
      }
    }
    allQgrams.groupBy(x => x).map(x => (x._1, x._2.size)).collectAsMap().toMap
  }

  /**
    * Ordina i qgrammi all'interno del documento per la loro document frequency
    **/
  def getSortedQgrams(docs: RDD[(Long, Array[(String, Int)])]): RDD[(Long, Array[(Int, Int)])] = {
    val tf = getQgramsTf(docs)
    val tf2 = docs.context.broadcast(tf.toList.sortBy(_._2).zipWithIndex.map(x => (x._1._1, x._2)).toMap)
    docs.map { case (docId, qgrams) =>
      val sortedQgrams = qgrams.map(q => (tf2.value(q._1), q._2)).sortBy(q => q)
      (docId, sortedQgrams)
    }
  }

  /**
    * Dato l'elenco di documenti con i q-grammi ordinati crea il prefix index.
    * Nota: per risolvere il problema dei documenti troppo corti il prefix index contiene un blocco identificato dall'id
    * specificato in "fixprefix" che contiene tutti i documenti che non possono essere verificati con sicurezza.
    **/
  def buildPrefixIndex(sortedDocs: RDD[(Long, Array[(Int, Int)])], qgramLen: Int, threshold: Int): RDD[(Int, Array[Qgram])] = {
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
    allQgrams.groupBy(_._1).filter(_._2.size > 1).map(x => (x._1, x._2.map(_._2).toArray))
  }
}
