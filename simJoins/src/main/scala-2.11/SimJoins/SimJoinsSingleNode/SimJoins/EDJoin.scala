package SimJoins.SimJoinsSingleNode.SimJoins

import java.io.PrintWriter
import java.util.Calendar

import SimJoins.SimJoinsSingleNode.Commons.CommonFunctions
import SimJoins.SimJoinsSingleNode.Commons.ED.CommonEdFunctions.commons
import SimJoins.SimJoinsSingleNode.Commons.ED.{CommonEdFunctions, EdFilters}
import SimJoins.DataStructure.Profile

/**
  * Implements the ED Join algorithm
  **/
object EDJoin {

  /**
    * @param profiles             set of profiles
    * @param attributesThresholds thresholds for each attribute
    * @param log                  logger
    * @param qgramLen             size of the q-grams
    **/
  def getMatchesMulti(profiles: List[Profile], attributesThresholds: Map[String, Double], log: PrintWriter, qgramLen: Int): List[(Long, Long)] = {
    /**
      * For each attribute in attributeThresholds, obtain the candidate set
      **/
    val tokenizedAndCandidates = attributesThresholds.map { case (attribute, threshold) =>
      log.println("[EDJoin] Attribute " + attribute)
      val t1 = Calendar.getInstance().getTimeInMillis
      val docs = CommonFunctions.extractField(profiles, attribute)
      val t2 = Calendar.getInstance().getTimeInMillis
      log.println("[EDJoin] Time to tokenize attribute's data " + CommonFunctions.msToMin(t2 - t1) + " min")
      val res = (attribute, EDJoin.getCandidates(docs, qgramLen, threshold.toInt, log))
      val t3 = Calendar.getInstance().getTimeInMillis
      log.println("[EDJoin] Time to compute candidate set " + CommonFunctions.msToMin(t3 - t2) + " min")
      res
    }


    val t4 = Calendar.getInstance().getTimeInMillis
    val tokenizedAttributes = tokenizedAndCandidates.map(x => (x._1, x._2._1))

    val a = tokenizedAttributes.toList.flatMap(x => x._2.map(y => (y._1, x._1, y._2)))
    val docTokens = a.groupBy(_._1).map(x => (x._1, x._2.map(y => (y._2, y._3)).toMap))

    val candidates = tokenizedAndCandidates.map(_._2._2).reduce((c1, c2) => c1.intersect(c2))


    log.println("[EDJoin] Candidates number " + candidates.length)

    val t5 = Calendar.getInstance().getTimeInMillis

    log.println("[EDJoin] Time to obtain candidates " + CommonFunctions.msToMin(t5 - t4))


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
          pass = CommonEdFunctions.editDist(docs1(attribute), docs2(attribute)) <= threshold
        }
      }
      else {
        pass = false
      }
      pass
    }

    val t6 = Calendar.getInstance().getTimeInMillis

    log.println("[EDJoin] Matches " + pairs.length)
    log.println("[EDJoin] Verification time " + CommonFunctions.msToMin(t6 - t5))

    /*val finalResults = matches.reduce((l1, l2) => l1.intersect(l2))
    log.println("Numero di record finali "+finalResults.length)*/
    log.flush()
    pairs
  }


  /**
    * Ritorna le coppie che hanno ED <= threshold
    **/
  def getCandidates(documents: List[(Long, String)], qgramLength: Int, threshold: Int, log: PrintWriter): (Map[Long, String], List[(Long, Long)], Long, Long) = {

    val t1 = Calendar.getInstance().getTimeInMillis

    //Trasforma i documenti in q-grammi
    val docs = documents.map(x => (x._1, CommonEdFunctions.getQgrams(x._2, qgramLength)))

    //Ordina i q-grammi per la loro document frequency
    val sortedDocs = CommonEdFunctions.getSortedQgrams(docs)

    //Costruisce il prefix index
    val prefixIndex = CommonEdFunctions.buildPrefixIndex(sortedDocs, qgramLength, threshold)
    val t2 = Calendar.getInstance().getTimeInMillis
    log.println("[EDJoin] Tempo di creazione dell'indice " + (t2 - t1))
    val tIndex = t2 - t1

    //Candidati
    var preCandidates: List[(Long, Long)] = Nil

    //Inizializzo l'array che mi dice se ho già visto o meno un vicino
    val maxId = sortedDocs.maxBy(_._1)._1.toInt + 1
    val neighbors = Array.ofDim[Int](maxId)
    val notFound = Array.fill[Boolean](maxId) {
      true
    }
    var numNeighbors = 0

    //Lunghezza del prefisso
    val prefixLen = EdFilters.getPrefixLen(qgramLength, threshold)


    val t3 = Calendar.getInstance().getTimeInMillis
    log.println("[EDJoin] Tempo inizializzazione " + (t3 - t2))

    //Per ogni documento
    sortedDocs.foreach { case (docId, qgrams) =>
      val docLen = qgrams.length
      //Prendo il suo prefisso, nel caso in cui sia troppo corto allora devo guardare anche il blocco speciale
      val prefix = {
        if (docLen < prefixLen) {
          qgrams.union(commons.fixPrefix :: Nil)
        }
        else {
          qgrams.take(prefixLen)
        }
      }

      //Per ogni elemento nel prefisso
      prefix.foreach { case (qgram, qgramPos) =>
        //Prendo il blocco relativo a quell'elemento (se esiste)
        val block = prefixIndex.get(qgram)
        if (block.isDefined) {
          //Per ogni vicino
          block.get.foreach { neighbor =>
            //Se il vicino non è già stato visto in precedenza
            if (docId < neighbor.docId && notFound(neighbor.docId.toInt)) {
              //Se passa il length filter e la posizione tra i due q-grammi è inferiore alla soglia
              if (Math.abs(neighbor.docLength - docLen) <= threshold && Math.abs(qgramPos - neighbor.qgramPos) <= threshold) {
                preCandidates = (docId, neighbor.docId) :: preCandidates
              }
              //Segna il vicino come già visto
              notFound.update(neighbor.docId.toInt, false)
              neighbors.update(numNeighbors, neighbor.docId.toInt)
              numNeighbors += 1
            }
          }
        }
      }
      //Alla fine del documento resetta i vicini visti
      for (i <- 0 until numNeighbors) {
        notFound.update(neighbors(i), true)
      }
      numNeighbors = 0
    }

    val sortedDocMap = sortedDocs.toMap
    val documentMap = documents.toMap

    log.println("[EDJoin] Numero pre-candidati (questi vengono tutti parsati dal common filter) " + preCandidates.length)

    val t4 = Calendar.getInstance().getTimeInMillis
    log.println("[EDJoin] Tempo precandidati " + (t4 - t3))

    //Ora la fase di verifica finale
    val candidates = preCandidates.filter { case (doc1Id, doc2Id) =>
      //Se passa il common filter allora calcola l'ED e verifica che sia inferiore/uguale alla soglia
      if (EdFilters.commonFilter(sortedDocMap(doc1Id), sortedDocMap(doc2Id), qgramLength, threshold)) {
        true
      }
      else {
        false
      }
    }

    val t5 = Calendar.getInstance().getTimeInMillis
    log.println("[EDJoin] Numero candidati " + candidates.length)
    log.println("[EDJoin] Tempo common filter " + (t5 - t4))
    log.println("[EDJoin] Tempo di JOIN complessivo " + (t5 - t3))
    val tJoin = t5 - t3

    (documentMap, candidates, tIndex, tJoin)
  }


  /**
    * Ritorna le coppie che hanno ED <= threshold
    **/
  def getMatches(documents: List[(Long, String)], qgramLength: Int, threshold: Int, log: PrintWriter): List[(Long, Long)] = {

    val t1 = Calendar.getInstance().getTimeInMillis
    //Trasforma i documenti in q-grammi
    val docs = documents.map(x => (x._1, CommonEdFunctions.getQgrams(x._2, qgramLength)))
    //Ordina i q-grammi per la loro document frequency
    val sortedDocs = CommonEdFunctions.getSortedQgrams(docs)

    val t2 = Calendar.getInstance().getTimeInMillis
    log.println("[EDJoin] Tempo preprocessing " + CommonFunctions.msToMin(t2 - t1))

    //Costruisce il prefix index
    val prefixIndex = CommonEdFunctions.buildPrefixIndex(sortedDocs, qgramLength, threshold)
    val t3 = Calendar.getInstance().getTimeInMillis
    log.println("[EDJoin] Tempo prefix indexing " + CommonFunctions.msToMin(t3 - t2))

    //Candidati
    var candidates: List[(Long, Long)] = Nil

    //Inizializzo l'array che mi dice se ho già visto o meno un vicino
    val maxId = sortedDocs.maxBy(_._1)._1.toInt + 1
    val neighbors = Array.ofDim[Int](maxId)
    val notFound = Array.fill[Boolean](maxId) {
      true
    }
    var numNeighbors = 0
    var lenFilterAct: Double = 0

    //Lunghezza del prefisso
    val prefixLen = EdFilters.getPrefixLen(qgramLength, threshold)

    //Per ogni documento
    sortedDocs.foreach { case (docId, qgrams) =>
      val docLen = qgrams.length
      //Prendo il suo prefisso, nel caso in cui sia troppo corto allora devo guardare anche il blocco speciale
      val prefix = {
        if (docLen < prefixLen) {
          qgrams.union(commons.fixPrefix :: Nil)
        }
        else {
          qgrams.take(prefixLen)
        }
      }

      //Per ogni elemento nel prefisso
      prefix.foreach { case (qgram, qgramPos) =>
        //Prendo il blocco relativo a quell'elemento (se esiste)
        val block = prefixIndex.get(qgram)
        if (block.isDefined) {
          //Per ogni vicino
          block.get.foreach { neighbor =>
            //Se il vicino non è già stato visto in precedenza
            if (docId < neighbor.docId && notFound(neighbor.docId.toInt)) {
              lenFilterAct += 1
              //Se passa il length filter e la posizione tra i due q-grammi è inferiore alla soglia
              if (Math.abs(neighbor.docLength - docLen) <= threshold && Math.abs(qgramPos - neighbor.qgramPos) <= threshold) {
                candidates = (docId, neighbor.docId) :: candidates
              }
              //Segna il vicino come già visto
              notFound.update(neighbor.docId.toInt, false)
              neighbors.update(numNeighbors, neighbor.docId.toInt)
              numNeighbors += 1
            }
          }
        }
      }
      //Alla fine del documento resetta i vicini visti
      for (i <- 0 until numNeighbors) {
        notFound.update(neighbors(i), true)
      }
      numNeighbors = 0
    }

    log.println("[EDJoin] Numero di attivazioni length filter " + lenFilterAct)
    log.println("[EDJoin] Numero di candidati (corrisponde anche al numero di attivazioni del commonFilter) " + candidates.length)

    val sortedDocMap = sortedDocs.toMap
    val documentMap = documents.toMap

    //Ora la fase di verifica finale
    val verified = candidates.filter { case (doc1Id, doc2Id) =>
      //Se passa il common filter allora calcola l'ED e verifica che sia inferiore/uguale alla soglia
      if (EdFilters.commonFilter(sortedDocMap(doc1Id), sortedDocMap(doc2Id), qgramLength, threshold) && CommonEdFunctions.editDist(documentMap(doc1Id), documentMap(doc2Id)) <= threshold) {
        true
      }
      else {
        false
      }
    }

    log.println("[EDJoin] Numero di coppie verificate " + verified.length)

    val t4 = Calendar.getInstance().getTimeInMillis
    log.println("[EDJoin] Tempo totale (min) " + CommonFunctions.msToMin(t4 - t3))


    verified
  }
}
