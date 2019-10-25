package SimJoins.SimJoinsSingleNode.SimJoins

import java.io.PrintWriter
import java.util.Calendar

import SimJoins.DataStructure.Profile
import SimJoins.SimJoinsSingleNode.Commons.CommonFunctions
import SimJoins.SimJoinsSingleNode.Commons.ED.CommonEdFunctions
import SimJoins.SimJoinsSingleNode.Experiments.MainAllAnd.algTypes

/**
  * Combinazione PPJoin + EDJoin
  **/
object PpEdJoin {

  /** Contiene i risultati dell'esecuzione del PPJoin/EDJoin */
  case class Results(algType: String, candidates: List[(Long, Long)], threshold: Double, EDJoinDocumentMap: Map[Long, String] = Map.empty[Long, String], PPJoinDocumentMap: Map[Long, Array[Int]] = Map.empty[Long, Array[Int]], tIndex: Long, tJoin: Long)


  /** Verifica con PPJOIN/EDJOIN un set di condizioni in AND tra loro */
  def getCandidatesAND(profiles: List[Profile], conditions: Map[String, (Double, String)], log: PrintWriter, qgramsLen: Int): (Array[Results], List[(Long, Long)], Long, Long) = {
    /** Contiene i set di risultati di ogni singola condizione */
    var candidates: List[(Long, Long)] = Nil
    /** Esegue tutte le condizioni nell'elenco */
    val data = conditions.map { case (attribute, (threshold, thresholdType)) =>
      log.println("[EPP] Algoritmo " + thresholdType + ", soglia " + threshold + ", attributo " + attribute)
      if (thresholdType == GraphJoinAND.thresholdTypes.JS) {
        val tmp = PPJoin.getCandidates(CommonFunctions.extractField(profiles, attribute), threshold, log)
        Results(algTypes.ppJoin, tmp._2, threshold, PPJoinDocumentMap = tmp._1, tIndex = tmp._3, tJoin = tmp._4)
      }
      else {
        val tmp = EDJoin.getCandidates(CommonFunctions.extractField(profiles, attribute), qgramsLen, threshold.toInt, log)
        Results(algTypes.editDistance, tmp._2, threshold, EDJoinDocumentMap = tmp._1, tIndex = tmp._3, tJoin = tmp._4)
      }
    }.toArray

    /** Interseca i risultati di ogni condizione */
    data.foreach { result =>
      if (candidates == Nil) {
        candidates = result.candidates
      }
      else {
        candidates = candidates.intersect(result.candidates)
      }
    }

    val totTIndex = data.map(r => r.tIndex).sum
    val totTJoin = data.map(r => r.tJoin).sum

    (data, candidates, totTIndex, totTJoin)
  }

  /**
    * Controlla un set di condizioni in AND per due documenti
    **/
  def checkAndCondition(doc1Id: Long, doc2Id: Long, data: Array[Results]): Boolean = {
    var pass = true
    var i = 0
    while (pass && i < data.length) {
      if (data(i).algType == algTypes.ppJoin) {
        val d1 = data(i).PPJoinDocumentMap.get(doc1Id)
        val d2 = data(i).PPJoinDocumentMap.get(doc2Id)
        pass = false
        if (d1.isDefined && d2.isDefined) {
          val doc1 = d1.get
          val doc2 = d2.get
          val common = doc1.intersect(doc2).length
          pass = (common.toDouble / (doc1.length + doc2.length - common)) >= data(i).threshold
        }
      }
      else {
        pass = CommonEdFunctions.editDist(data(i).EDJoinDocumentMap(doc1Id), data(i).EDJoinDocumentMap(doc2Id)) <= data(i).threshold
      }
      i += 1
    }
    pass
  }

  /**
    * Esegue le condizioni utilizzando PPJoin ed EDJoin e intersecando i risultati dei due
    **/
  def getMatchesAndOr(profiles: List[Profile], conditions: List[Map[String, (Double, String)]], log: PrintWriter, qgramsLen: Int = 2): List[(Long, Long)] = {
    /** Esegue il set di condizioni in AND tra di loro */
    val res = for (i <- conditions.indices) yield {
      getCandidatesAND(profiles, conditions(i), log, qgramsLen)
    }

    val tTotIndex = res.map(_._3).sum
    val tTotJoin = res.map(_._4).sum

    /** Unisce i candidati per le varie condizioni in AND */
    val candidates = res.map(_._2).reduce((l1, l2) => l1.union(l2)).distinct

    /** Contiene i dati per eseguire la verifica (documenti tokenizzati) */
    val data = res.map(_._1)

    /** Verifica che almeno una delle condizioni AND sia passata */
    val t1 = Calendar.getInstance().getTimeInMillis
    val results = candidates.filter { case (doc1Id, doc2Id) =>
      var continue = true
      val it = data.iterator
      while (it.hasNext && continue) {
        val passAnd = checkAndCondition(doc1Id, doc2Id, it.next())
        continue = !passAnd
      }
      !continue
    }
    val t2 = Calendar.getInstance().getTimeInMillis

    log.println("[PpEdJoin] Tempo di indicizzazione " + tTotIndex)
    log.println("[PpEdJoin] Tempo di join " + tTotJoin)
    log.println("[PpEdJoin] Tempo di verifica" + (t2 - t1))

    log.println("[PpEdJoin] STAT: " + tTotIndex + "," + tTotJoin + "," + (t2 - t1))

    results
  }
}
