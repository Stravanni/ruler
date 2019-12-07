package RulER.SimJoins

import java.util.Calendar

import RulER.DataStructure.Profile
import RulER.Commons.CommonFunctions
import RulER.Commons.ED.{CommonEdFunctions, EdFilters}
import RulER.DataStructure.CommonClasses.tokensED
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD

object EDJoin {
  /**
    * @param profiles             set of profiles
    * @param attributesThresholds thresholds for each attribute
    * @param qgramLen             size of the q-grams
    **/
  def getMatchesMulti(profiles: RDD[Profile], attributesThresholds: Map[String, Double], qgramLen: Int): RDD[(Long, Long)] = {
    /**
      * For each attribute in attributeThresholds, obtain the candidate set
      **/
    val candidates = attributesThresholds.map { case (attribute, threshold) =>
      val docs = CommonFunctions.extractField(profiles, attribute)
      val cand = getCandidates(docs, qgramLen, threshold.toInt)
      //todo: anche questo fa schifo
      val res = (attribute, cand.join(docs).map(d => (d._2._1, (d._1, d._2._2))).join(docs).map(d => (d._2._1, (d._1, d._2._2))))
      res
    }

    val conditionsNum = attributesThresholds.size

    val a = candidates.toList.map { case (attribute, pairs1) =>
      pairs1.map { case (doc1, doc2) =>
        val key = (doc1._1, doc2._1)
        val value = (attribute, (doc1._2, doc2._2))
        (key, value)
      }
    }.reduce((x, y) => x.union(y)).groupByKey().filter(_._2.size == conditionsNum)


    val pairs = a.filter { case (docIds, attributeValues) =>
      var pass = true
      val attributeValuesMap = attributeValues.toMap
      val it = attributesThresholds.iterator
      while (it.hasNext && pass) {
        val (attribute, threshold) = it.next()
        pass = CommonEdFunctions.editDist(attributeValuesMap(attribute)._1, attributeValuesMap(attribute)._2) <= threshold
      }

      pass
    }.map(_._1)

    pairs
  }


  /**
    * Ritorna le coppie che hanno ED <= threshold
    **/
  def getCandidates(documents: RDD[(Long, String)], qgramLength: Int, threshold: Int): RDD[(Long, Long)] = {

    //Trasforma i documenti in q-grammi
    val docs = documents.map(x => (x._1, CommonEdFunctions.getQgrams(x._2, qgramLength)))

    //Ordina i q-grammi per la loro document frequency
    val sortedDocs = CommonEdFunctions.getSortedQgrams(docs)

    //Costruisce il prefix index
    val ts = Calendar.getInstance().getTimeInMillis
    val prefixIndex = documents.context.broadcast(CommonEdFunctions.buildPrefixIndex(sortedDocs, qgramLength, threshold).collectAsMap())
    val te = Calendar.getInstance().getTimeInMillis
    val log = LogManager.getRootLogger
    log.info("[GraphJoin] EDJOIN index time (s) " + (te - ts) / 1000.0)

    val t1 = Calendar.getInstance().getTimeInMillis
    //Id massimo
    val maxId = sortedDocs.map(_._1).max().toInt + 1

    //Lunghezza del prefisso
    val prefixLen = CommonEdFunctions.getPrefixLen(qgramLength, threshold)

    val preCandidates = sortedDocs.mapPartitions { partition =>
      //Inizializzo l'array che mi dice se ho già visto o meno un vicino
      val neighbors = Array.ofDim[Int](maxId)
      val notFound = Array.fill[Boolean](maxId) {
        true
      }
      var numNeighbors = 0
      //Candidati
      var partPreCandidates: List[(Long, Long)] = Nil

      partition.foreach { case (docId, qgrams) =>
        val docLen = qgrams.length
        //Prendo il suo prefisso, nel caso in cui sia troppo corto allora devo guardare anche il blocco speciale
        val prefix = {
          if (docLen < prefixLen) {
            qgrams.union(CommonEdFunctions.commons.fixPrefix :: Nil)
          }
          else {
            qgrams.take(prefixLen)
          }
        }

        //Per ogni elemento nel prefisso
        prefix.foreach { case (qgram, qgramPos) =>
          //Prendo il blocco relativo a quell'elemento (se esiste)
          val block = prefixIndex.value.get(qgram)
          if (block.isDefined) {
            //Per ogni vicino
            block.get.foreach { neighbor =>
              //Se il vicino non è già stato visto in precedenza
              if (docId < neighbor.docId && notFound(neighbor.docId.toInt)) {
                //Se passa il length filter e la posizione tra i due q-grammi è inferiore alla soglia
                if (Math.abs(neighbor.docLength - docLen) <= threshold && Math.abs(qgramPos - neighbor.qgramPos) <= threshold) {
                  partPreCandidates = (docId, neighbor.docId) :: partPreCandidates
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
      partPreCandidates.toIterator
    }

    preCandidates.cache()
    preCandidates.count()
    val tk = Calendar.getInstance().getTimeInMillis
    log.info("[GraphJoin] Tempo precandidati (s)" + (tk - t1) / 1000.0)

    //TODO: non è un gran mossa questa, però non so come fare... anche mandare tutto in broadcast è un casino
    val docsBrd = preCandidates.context.broadcast(sortedDocs.collectAsMap())

    //Ora la fase di verifica finale
    val candidates = preCandidates.filter { case (doc1, doc2) =>
      val d1 = docsBrd.value.get(doc1)
      val d2 = docsBrd.value.get(doc1)
      if (d1.isDefined && d2.isDefined) {
        //Se passa il common filter allora calcola l'ED e verifica che sia inferiore/uguale alla soglia
        if (EdFilters.commonFilter(d1.get, d2.get, qgramLength, threshold)) {
          true
        }
        else {
          false
        }
      }
      else {
        false
      }
    }
    /*val predocs = sortedDocs.join(preCandidates).map(p => (p._2._2, (p._1, p._2._1))).join(sortedDocs).map(x => (x._2._1, (x._1, x._2._2)))

    val sortedDocMap = sortedDocs

    //Ora la fase di verifica finale
    val candidates = predocs.filter { case (doc1, doc2) =>
      //Se passa il common filter allora calcola l'ED e verifica che sia inferiore/uguale alla soglia
      if (EdFilters.commonFilter(doc1._2, doc2._2, qgramLength, threshold)) {
        true
      }
      else {
        false
      }
    }
    */

    candidates.count()
    preCandidates.unpersist()
    val t2 = Calendar.getInstance().getTimeInMillis
    log.info("[GraphJoin] EDJOIN join time (s) " + (t2 - t1) / 1000.0)

    candidates
  }


  def getCandidates2(sortedDocs: RDD[(Long, Array[(Int, Int)])], qgramLength: Int, threshold: Int): RDD[(Long, Long)] = {
    //Costruisce il prefix index
    val ts = Calendar.getInstance().getTimeInMillis
    val prefixIndex = sortedDocs.context.broadcast(CommonEdFunctions.buildPrefixIndex(sortedDocs, qgramLength, threshold).collectAsMap())
    val te = Calendar.getInstance().getTimeInMillis
    val log = LogManager.getRootLogger
    log.info("[GraphJoin] EDJOIN index time (s) " + (te - ts) / 1000.0)

    val t1 = Calendar.getInstance().getTimeInMillis
    //Id massimo
    val maxId = sortedDocs.map(_._1).max().toInt + 1

    //Lunghezza del prefisso
    val prefixLen = CommonEdFunctions.getPrefixLen(qgramLength, threshold)

    val preCandidates = sortedDocs.mapPartitions { partition =>
      //Inizializzo l'array che mi dice se ho già visto o meno un vicino
      val neighbors = Array.ofDim[Int](maxId)
      val notFound = Array.fill[Boolean](maxId) {
        true
      }
      var numNeighbors = 0
      //Candidati
      var partPreCandidates: List[(Long, Long)] = Nil

      partition.foreach { case (docId, qgrams) =>
        val docLen = qgrams.length
        //Prendo il suo prefisso, nel caso in cui sia troppo corto allora devo guardare anche il blocco speciale
        val prefix = {
          if (docLen < prefixLen) {
            qgrams.union(CommonEdFunctions.commons.fixPrefix :: Nil)
          }
          else {
            qgrams.take(prefixLen)
          }
        }

        //Per ogni elemento nel prefisso
        prefix.foreach { case (qgram, qgramPos) =>
          //Prendo il blocco relativo a quell'elemento (se esiste)
          val block = prefixIndex.value.get(qgram)
          if (block.isDefined) {
            //Per ogni vicino
            block.get.foreach { neighbor =>
              //Se il vicino non è già stato visto in precedenza
              if (docId < neighbor.docId && notFound(neighbor.docId.toInt)) {
                //Se passa il length filter e la posizione tra i due q-grammi è inferiore alla soglia
                if (Math.abs(neighbor.docLength - docLen) <= threshold && Math.abs(qgramPos - neighbor.qgramPos) <= threshold) {
                  partPreCandidates = (docId, neighbor.docId) :: partPreCandidates
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
      partPreCandidates.toIterator
    }

    preCandidates.cache()
    preCandidates.count()
    val tk = Calendar.getInstance().getTimeInMillis
    log.info("[GraphJoin] Tempo precandidati (s)" + (tk - t1) / 1000.0)

    //TODO: non è un gran mossa questa, però non so come fare... anche mandare tutto in broadcast è un casino
    val docsBrd = preCandidates.context.broadcast(sortedDocs.collectAsMap())

    //Ora la fase di verifica finale
    val candidates = preCandidates.filter { case (doc1, doc2) =>
      val d1 = docsBrd.value.get(doc1)
      val d2 = docsBrd.value.get(doc1)
      if (d1.isDefined && d2.isDefined) {
        //Se passa il common filter allora calcola l'ED e verifica che sia inferiore/uguale alla soglia
        if (EdFilters.commonFilter(d1.get, d2.get, qgramLength, threshold)) {
          true
        }
        else {
          false
        }
      }
      else {
        false
      }
    }

    candidates.count()
    preCandidates.unpersist()
    val t2 = Calendar.getInstance().getTimeInMillis
    log.info("[GraphJoin] EDJOIN join time (s) " + (t2 - t1) / 1000.0)

    candidates
  }
}
