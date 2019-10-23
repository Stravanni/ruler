package SimJoins.SimJoinsSingleNode.Experiments

import java.io.{File, PrintWriter}
import java.util.Calendar

import SimJoins.SimJoinsSingleNode.Commons.CommonFunctions
import SimJoins.SimJoinsSingleNode.Commons.ED.CommonEdFunctions
import SimJoins.DataStructure.Profile
import SimJoins.SimJoinsSingleNode.SimJoins.{EDJoin, GraphJoinAND, PPJoin}

object MainAll {

  /**
    * Tipi di algoritmi
    **/
  object algTypes {
    val editDistance = "E"
    val graphJoin = "G"
    val ppJoin = "PP"
    val editPP = "EPP"
  }

  def saveResults(pairs: List[(Long, Long)], outpath: String): Unit = {
    val p = new PrintWriter(new File(outpath))
    pairs.foreach { case (d1, d2) => p.println(d1 + "," + d2) }
    p.close()
  }

  /**
    * Carica le condizioni
    * Il formato di una condizione è:
    * attributo|tipo|soglia
    **/
  def loadConditions(conditions: String): Map[String, (Double, String)] = {
    conditions.split(",").map { condition =>
      val c = condition.split("\\|")
      (c.head, (c.last.toDouble, c(1)))
    }.toMap
  }


  case class Results(algType: String, candidates: List[(Long, Long)], threshold: Double, EDJoinDocumentMap: Map[Long, String] = Map.empty[Long, String], PPJoinDocumentMap: Map[Long, Array[Int]] = Map.empty[Long, Array[Int]])

  /**
    * Esegue le condizioni utilizzando PPJoin ed EDJoin e intersecando i risultati dei due
    **/
  def getMatchesMultiMixedAdv(profiles: List[Profile], conditions: Map[String, (Double, String)], log: PrintWriter, qgramsLen: Int): List[(Long, Long)] = {

    var candidates: List[(Long, Long)] = Nil

    val data = conditions.map { case (attribute, (threshold, thresholdType)) =>
      log.println("[EPP] Algoritmo " + thresholdType + ", soglia " + threshold + ", attributo " + attribute)
      if (thresholdType == GraphJoinAND.thresholdTypes.JS) {
        val tmp = PPJoin.getCandidates(CommonFunctions.extractField(profiles, attribute), threshold, log)
        Results(algTypes.ppJoin, tmp._2, threshold, PPJoinDocumentMap = tmp._1)
      }
      else {
        val tmp = EDJoin.getCandidates(CommonFunctions.extractField(profiles, attribute), qgramsLen, threshold.toInt, log)
        Results(algTypes.editDistance, tmp._2, threshold, EDJoinDocumentMap = tmp._1)
      }
    }.toArray

    data.foreach { result =>
      if (candidates == Nil) {
        candidates = result.candidates
      }
      else {
        candidates = candidates.intersect(result.candidates)
      }
    }

    candidates.filter { case (doc1Id, doc2Id) =>
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
  }

  /**
    * Esegue le condizioni utilizzando PPJoin ed EDJoin e intersecando i risultati dei due
    **/
  def getMatchesMultiMixed(profiles: List[Profile], conditions: Map[String, (Double, String)], log: PrintWriter, qgramsLen: Int): List[(Long, Long)] = {

    var results: List[(Long, Long)] = Nil

    conditions.foreach { case (attribute, (threshold, thresholdType)) =>

      log.println("[EPP] Algoritmo " + thresholdType + ", soglia " + threshold + ", attributo " + attribute)

      val pairs = {
        if (thresholdType == GraphJoinAND.thresholdTypes.JS) {
          PPJoin.getMatches(CommonFunctions.extractField(profiles, attribute), threshold, log)
        }
        else {
          EDJoin.getMatches(CommonFunctions.extractField(profiles, attribute), qgramsLen, threshold.toInt, log)
        }
      }
      if (results == Nil) {
        results = pairs
      }
      else {
        results = pairs.intersect(results)
      }
    }

    results
  }

  def main(args: Array[String]): Unit = {
    val algType = args(0)
    val logPath = args(1)
    val conditionsStr = args(2).toString
    val dataPath = args(3)
    val sort = {
      if (args.length >= 5) {
        args(4).toInt
      }
      else {
        1
      }
    }

    val log = new PrintWriter(new File(logPath))
    log.println("AlgType " + algType)
    log.println("Conditions " + conditionsStr)
    log.println("Dataset " + dataPath)
    log.println("Sort " + sort)
    log.flush()

    val conditions = loadConditions(conditionsStr)
    val profiles = CommonFunctions.loadData(dataPath)
    log.println("Numero profili " + profiles.length)
    log.flush()

    val startTime = Calendar.getInstance()

    val pairs = {
      if (algType == algTypes.editDistance) {
        EDJoin.getMatchesMulti(profiles, conditions.map(c => (c._1, c._2._1)), log, 2)
      }
      else if (algType == algTypes.ppJoin) {
        PPJoin.getMatchesMulti(profiles, conditions.map(c => (c._1, c._2._1)), log)
      }
      else if (algType == algTypes.graphJoin) {
        val sortType = {
          if (sort == 1) {
            GraphJoinAND.sortTypes.thresholdAsc
          }
          else if (sort == 2) {
            GraphJoinAND.sortTypes.thresholdDesc
          }
          else if (sort == 3) {
            GraphJoinAND.sortTypes.avgBlockSizeAsc
          }
          else if (sort == 4) {
            GraphJoinAND.sortTypes.avgBlockSizeDesc
          }
          else if (sort == 5) {
            GraphJoinAND.sortTypes.entroAsc
          }
          else {
            GraphJoinAND.sortTypes.entroDesc
          }
        }

        log.println("Sort " + sortType)

        GraphJoinAND.getMatchesMultiSort(profiles, conditions, log, sortType)
      }
      else {
        getMatchesMultiMixedAdv(profiles, conditions, log, 2)
      }
    }

    //saveResults(pairs, "/data2/res"+algType+".txt")

    log.println("Numero risultati " + pairs.length)
    val endTime = Calendar.getInstance()
    log.println("Tempo esecuzione totale (min) " + CommonFunctions.msToMin(endTime.getTimeInMillis - startTime.getTimeInMillis))
    log.close()
  }
}
