package SimJoins.SimJoinsSingleNode.SimJoins

import java.io.PrintWriter
import java.util.Calendar

import SimJoins.SimJoinsSingleNode.Commons.CommonFunctions
import SimJoins.SimJoinsSingleNode.Commons.JS.CommonJsFunctions
import SimJoins.SimJoinsSingleNode.Commons.JS.JsFilters
import SimJoins.DataStructure.{PrefixEntry, PrefixIndex, Profile}

object PPJoin {

  def getMatchesMulti(profiles: List[Profile], attributesThresholds: Map[String, Double], log: PrintWriter): List[(Long, Long)] = {
    val tokenizedAndCandidates = attributesThresholds.map { case (attribute, threshold) =>
      log.println("Attribute: " + attribute)
      val docs = CommonFunctions.extractField(profiles, attribute)
      (attribute, PPJoin.getCandidates(docs, threshold, log))
    }

    val tokenizedAttributes = tokenizedAndCandidates.map(x => (x._1, x._2._1))

    val a = tokenizedAttributes.toList.flatMap(x => x._2.toList.map(y => (y._1, x._1, y._2)))
    val docTokens = a.groupBy(_._1).map(x => (x._1, x._2.map(y => (y._2, y._3)).toMap))

    val candidates = tokenizedAndCandidates.map(_._2._2).reduce((c1, c2) => c1.intersect(c2))

    log.println("Candidati finali " + candidates.length)


    def check(doc1: Array[Int], doc2: Array[Int], threshold: Double): Boolean = {
      val common = doc1.intersect(doc2).length
      (common.toDouble / (doc1.length + doc2.length - common)) >= threshold
    }


    val pairs = candidates.filter { case (doc1, doc2) =>
      val d1 = docTokens.get(doc1)
      val d2 = docTokens.get(doc2)
      var pass = true
      if (d1.isDefined && d2.isDefined) {
        val docs1 = d1.get
        val docs2 = d2.get
        val it = attributesThresholds.iterator
        while (it.hasNext && pass) {
          val (attribute, threshold) = it.next()
          pass = check(docs1(attribute), docs2(attribute), threshold)
        }
      }
      else {
        pass = false
      }
      pass
    }

    /*val finalResults = matches.reduce((l1, l2) => l1.intersect(l2))
    log.println("Numero di record finali "+finalResults.length)*/
    log.flush()
    pairs
  }

  def getCandidates(documents: List[(Long, String)], threshold: Double, log: PrintWriter): (Map[Long, Array[Int]], List[(Long, Long)]) = {

    val tokenizedDocSort = CommonJsFunctions.tokenizeAndSort(documents).toMap
    val prefixIndex = CommonJsFunctions.buildPrefixIndex(tokenizedDocSort.toList, threshold)

    //val candidates = new mutable.HashSet[(Long, Long)]
    var candidates: List[(Long, Long)] = Nil
    var comparisonNumbers = 0D

    tokenizedDocSort.filter(_._2.length > 0).foreach { case (docId, tokens) =>
      val docLen = tokens.length
      for (i <- 0 until JsFilters.getPrefixLength(docLen, threshold)) {
        val blockId = tokens(i)
        val block = prefixIndex.get(blockId)
        if (block.isDefined) {
          val neighbors = block.get.filter(n => docId < n.docId && Math.min(docLen, n.docLen) >= Math.min(docLen, n.docLen) * threshold)
          neighbors.foreach { neighbor =>
            comparisonNumbers += 1
            val commonTokens = CommonJsFunctions.getCommonPrefixElements(blockId, tokens, tokenizedDocSort(neighbor.docId), threshold)
            if (commonTokens > 0 && JsFilters.positionFilter(docLen, neighbor.docLen, i + 1, neighbor.tokenPos, commonTokens, threshold)) {
              candidates = (docId, neighbor.docId) :: candidates
            }
          }
        }
      }
    }

    log.println("Numero di volte che ha attivato il position filter " + comparisonNumbers)
    log.println("Numero di candidati " + candidates.length)
    log.flush()
    (tokenizedDocSort, candidates)
  }

  def getMatches(documents: List[(Long, String)], threshold: Double, log: PrintWriter): List[(Long, Long)] = {


    val t1 = Calendar.getInstance()
    val tokenizedDocSort = CommonJsFunctions.tokenizeAndSort(documents).toMap
    val t2 = Calendar.getInstance()
    log.println("[PPJoin] Tempo preprocessing (min) " + CommonFunctions.msToMin(t2.getTimeInMillis - t1.getTimeInMillis))


    val startTime = Calendar.getInstance()

    val prefixIndex = new PrefixIndex()

    //val candidates = new mutable.HashSet[(Long, Long)]
    var candidates: List[(Long, Long)] = Nil
    var posFilterActivations: Double = 0

    tokenizedDocSort.filter(_._2.length > 0).foreach { case (docId, tokens) =>
      val docLen = tokens.length
      for (i <- 0 until JsFilters.getPrefixLength(docLen, threshold)) {
        val blockId = tokens(i)
        val block = prefixIndex.getBlock(blockId)
        if (block.isDefined) {
          /** Applica length filter */
          val neighbours = block.get.filter(n => Math.min(docLen, n.docLen) >= Math.min(docLen, n.docLen) * threshold)
          neighbours.foreach { neighbour =>
            val commonTokens = CommonJsFunctions.getCommonPrefixElements(blockId, tokens, tokenizedDocSort(neighbour.docId), threshold)
            posFilterActivations += 1

            /** Applica position filter */
            if (commonTokens > 0 && JsFilters.positionFilter(docLen, neighbour.docLen, i + 1, neighbour.tokenPos, commonTokens, threshold)) {
              if (docId < neighbour.docId) {
                //candidates.add((docId, neighbour.docId))
                candidates = (docId, neighbour.docId) :: candidates
              }
              else {
                candidates = (neighbour.docId, docId) :: candidates
              }
            }
          }
        }
        prefixIndex.addElement(blockId, PrefixEntry(docId, i + 1, docLen))
      }
    }

    val endCandTime = Calendar.getInstance()

    log.println("[PPJoin] Numero attivazioni position filter " + posFilterActivations)
    log.println("[PPJoin] Tempo generazione candidati (no preprocessing) (min) " + CommonFunctions.msToMin(endCandTime.getTimeInMillis - startTime.getTimeInMillis))
    log.println("[PPJoin] Numero di coppie candidate "+candidates.length)

    val pairs = candidates.filter { case (doc1Id, doc2Id) =>
      val d1 = tokenizedDocSort.get(doc1Id)
      val d2 = tokenizedDocSort.get(doc2Id)
      var result = false
      if (d1.isDefined && d2.isDefined) {
        val doc1 = d1.get
        val doc2 = d2.get
        val common = doc1.intersect(doc2).length
        result = (common.toDouble / (doc1.length + doc2.length - common)) >= threshold
      }
      result
    }

    val endTime = Calendar.getInstance()

    log.println("[PPJoin] Numero di coppie verificate "+pairs.length)
    log.println("[PPJoin] Tempo totale (min) " + CommonFunctions.msToMin(endTime.getTimeInMillis - startTime.getTimeInMillis))

    pairs
  }
}
