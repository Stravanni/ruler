package RulER.SimJoins

import java.util.Calendar

import RulER.Commons.JS.JsFilters
import RulER.DataStructure.MyPartitioner2
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD

object PPJoin2 {
  /**
    * Ritorna la posizione dell'ultimo token in comune nei due documenti se il blocco in cui si sono trovati
    * corrisponde all'ultimo.
    *
    * @param doc1Tokens   token del primo documento
    * @param doc2Tokens   token del secondo documento
    * @param currentToken id del prefix index corrente (l'id del blocco in cui si sono trovati)
    * @param prefixLen1   lunghezza prefix del primo documento
    * @param prefixLen2   lunghezza prefix del secondo documento
    **/
  def lastCommonTokenPosition(doc1Tokens: Array[Long], doc2Tokens: Array[Long], currentToken: Long, prefixLen1: Int, prefixLen2: Int): (Int, Int, Boolean) = {
    var d1Index = prefixLen1 - 1
    var d2Index = prefixLen2 - 1
    var valid = true
    var continue = true

    /**
      * Partendo dal prefisso scorro indietro i due documenti.
      * Sicuramente hanno un token in comune
      **/
    while (d1Index >= 0 && d2Index >= 0 && continue) {
      /**
        * Trovato un token comune
        **/
      if (doc1Tokens(d1Index) == doc2Tokens(d2Index)) {
        /**
          * Se il token è uguale al token che ha generato il blocco, allora termina il processo e lo considera valido
          **/
        if (currentToken == doc1Tokens(d1Index)) {
          continue = false
        }
        else {
          /**
            * Se è diverso non è valido, questo serve ad evitare di emettere duplicati
            **/
          continue = false
          valid = false
        }
      }

      /**
        * Visto che sono ordinati in base alla dimensione di uno o dell'altro, sposto gli indici
        **/
      else if (doc1Tokens(d1Index) > doc2Tokens(d2Index)) {
        d1Index -= 1
      }
      else {
        d2Index -= 1
      }
    }
    (d1Index, d2Index, valid)
  }

  /**
    * Conta gli elementi comuni nel prefisso
    *
    * @param doc1Tokens token del primo documento
    * @param doc2Tokens token del secondo documento
    * @param sPos1      posizione di partenza nel primo documento (la posizione dell'ultimo token comune nel prefisso)
    * @param sPos2      posizione di partenza nel secondo documento (la posizione dell'ultimo token comune nel prefisso)
    **/
  def getCommonElementsInPrefix(doc1Tokens: Array[Long], doc2Tokens: Array[Long], sPos1: Int, sPos2: Int): Int = {
    var common = 1
    var p1 = sPos1 - 1
    var p2 = sPos2 - 1
    while (p1 >= 0 && p2 >= 0) {
      if (doc1Tokens(p1) == doc2Tokens(p2)) {
        common = common + 1
        p1 -= 1
        p2 -= 1
      }
      else if (doc1Tokens(p1) > doc2Tokens(p2)) {
        p1 -= 1
      }
      else {
        p2 -= 1
      }
    }
    common
  }


  /**
    * Ritorna le coppie candidate che passano il length filter e il position usando il prefix filter
    **/
  def getCandidatePairs(prefixIndex: RDD[(Long, Array[(Long, Array[Long])])], threshold: Double): RDD[(Long, Long)] = {
    /**
      * Definisce partitioner custom, ripartiziona in base al numero di possibili coppie che possono essere generate
      * da ogni blocco
      **/
    val customPartitioner = new MyPartitioner2(prefixIndex.getNumPartitions)
    val repartitionIndex = prefixIndex.map(_.swap).partitionBy(customPartitioner)

    repartitionIndex.flatMap {
      case (docs, tokenId) =>
        var results: List[(Long, Long)] = Nil
        var i = 0
        while (i < docs.length - 1) {
          var j = i + 1
          val doc1Id = docs(i)._1
          val doc1Tokens = docs(i)._2
          val doc1PrefixLen = JsFilters.getPrefixLength(doc1Tokens.length, threshold)

          /** Per ogni coppia che passa il length filter */
          while ((j < docs.length) && (doc1Tokens.length >= docs(j)._2.length * threshold)) { //Length filter
            val doc2Id = docs(j)._1
            val doc2Tokens = docs(j)._2
            val doc2PrefixLen = JsFilters.getPrefixLength(doc2Tokens.length, threshold)
            /** Guarda se il token corrente è l'ultimo, in questo modo viene emesso solo una volta */
            val (p1, p2, isLastCommon) = lastCommonTokenPosition(doc1Tokens, doc2Tokens, tokenId, doc1PrefixLen, doc2PrefixLen)

            if (isLastCommon) {
              val common = getCommonElementsInPrefix(doc1Tokens, doc2Tokens, p1, p2) //Prende elementi comuni nel prefisso
              if (JsFilters.positionFilter(doc1Tokens.length, doc2Tokens.length, p1 + 1, p2 + 1, common, threshold)) { //Verifica che passi il position filter
                if (doc1Id < doc2Id) {
                  results = (doc1Id, doc2Id) :: results
                }
                else {
                  results = (doc2Id, doc1Id) :: results
                }
              }
            }
            j = j + 1
          }
          i += 1
        }
        results
    }
  }


  /**
    * Crea un inverted index che per ogni documento per ogni token che ha nel prefisso ritorna
    * (docId, token del documento)
    **/
  def buildPrefixIndex(tokenizedDocOrd: RDD[(Long, Array[Long])], threshold: Double): RDD[(Long, Array[(Long, Array[Long])])] = {
    val indices = tokenizedDocOrd.flatMap {
      case (docId, tokens) =>
        val prefix = JsFilters.getPrefix(tokens, threshold)
        prefix.zipWithIndex.map {
          case (token, pos) =>
            (token, (docId, tokens))
        }
    }

    indices.groupByKey().filter(_._2.size > 1).map {
      case (tokenId, documents) => (tokenId, documents.toArray.sortBy(x => x._2.length))
    }
  }

  /** Ritorna l'elenco di coppie candidate */
  def getCandidates(tokenizedDocSort: RDD[(Long, Array[Long])], threshold: Double): RDD[(Long, Long)] = {
    val ts = Calendar.getInstance().getTimeInMillis
    val prefixIndex = buildPrefixIndex(tokenizedDocSort, threshold)
    prefixIndex.count()
    val log = LogManager.getRootLogger
    val te = Calendar.getInstance().getTimeInMillis
    log.info("[GraphJoin] PPJOIN index time (s) " + (te - ts) / 1000.0)

    val t1 = Calendar.getInstance().getTimeInMillis
    val candidates = getCandidatePairs(prefixIndex, threshold)
    candidates.count()
    val t2 = Calendar.getInstance().getTimeInMillis
    log.info("[GraphJoin] PPJOIN join time (s) " + (t2 - t1) / 1000.0)
    candidates
  }
}
