package SimJoins.SimJoinsSingleNode.SimJoins

import java.io.PrintWriter
import java.util.Calendar

import SimJoins.SimJoinsSingleNode.Commons.CommonFunctions
import SimJoins.SimJoinsSingleNode.Commons.ED.{CommonEdFunctions, EdFilters}
import SimJoins.SimJoinsSingleNode.Commons.ED.CommonEdFunctions.commons
import SimJoins.SimJoinsSingleNode.Commons.JS.CommonJsFunctions
import SimJoins.SimJoinsSingleNode.Commons.JS.JsFilters
import SimJoins.DataStructure.{PrefixEntry, Profile, Qgram}

object GraphJoinAND {

  object sortTypes {
    val avgBlockSizeAsc = "Ordinato per dimensione media blocchi crescente"
    val avgBlockSizeDesc = "Ordinato per dimensione media blocchi descrescente"
    val thresholdAsc = "Ordinato per soglia crescente"
    val thresholdDesc = "Ordinato per soglia decrescente"
    val entroAsc = "Crescente per entropia"
    val entroDesc = "Decrescente per entropia"
  }

  object thresholdTypes {
    val ED = "ED"
    val JS = "JS"
  }

  trait docData {
    def getType(): String
  }

  case class docJsData(attribute: String, prefixIndex: Map[Int, List[PrefixEntry]], tokenizedDocs: List[(Long, Array[Int])]) extends docData {
    def getType(): String = thresholdTypes.JS
  }

  case class docEdData(attribute: String, prefixIndex: Map[Int, List[Qgram]], qgrams: List[(Long, String, Array[(Int, Int)])]) extends docData {
    def getType(): String = thresholdTypes.ED
  }

  trait tokenized {
    def getType(): String
  }

  case class tokensJs(tokens: Array[Int]) extends tokenized {
    def getType(): String = thresholdTypes.JS
  }

  case class tokensED(originalStr: String, qgrams: Array[(Int, Int)]) extends tokenized {
    def getType(): String = thresholdTypes.ED
  }

  def parseConditions(conditions: List[Map[String, (Double, String)]]): Map[String, Map[String, Set[Double]]] = {
    val allConditions = conditions.flatten
    val conditionsPerAttribute = allConditions.groupBy(_._1)
    conditionsPerAttribute.map { case (attribute, conditions) =>

      val singleConditions = conditions.toSet
      val thresholds = singleConditions.map(_._2.swap)
      val thresholdsPerConditionType = thresholds.groupBy(_._1).map(x => (x._1, x._2.map(_._2)))

      (attribute, thresholdsPerConditionType)
    }
  }

  /**
    * Esegue il calcolo per l'edit distance
    **/
  def computeED(
                 qgrams: Array[(Int, Int)],
                 qgramLength: Int,
                 threshold: Int,
                 prefixIndexED: Map[String, Map[Int, List[Qgram]]],
                 attribute: String,
                 docId: Long,
                 cbs: Array[Int],
                 neighbors: Array[(Long, Int)],
                 numneighborsExt: Int,
                 numExcludedExt: Int,
                 excluded: Array[Int],
                 pos: Array[Int],
                 myPos: Array[Int]
               ): (Int, Int, Int) = {
    var numneighbors = numneighborsExt
    var numExcluded = numExcludedExt
    val prefixLen = EdFilters.getPrefixLen(qgramLength, threshold.toInt)
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
    for (i <- prefix.indices) {
      val qgram = prefix(i)._1
      val qgramPos = prefix(i)._2
      //Prendo il blocco relativo a quell'elemento (se esiste)
      val block = prefixIndexED(attribute).get(qgram)
      if (block.isDefined) {
        val validneighbors = block.get.filter(n => docId < n.docId && cbs(n.docId.toInt) >= 0)

        /** Per ogni vicino valido */
        validneighbors.foreach { neighbor =>
          val nId = neighbor.docId.toInt

          /** Se il CBS è 0 è la prima volta che lo trovo */
          if (cbs(nId) == 0) {
            /** Essendo la prima volta che lo incontro lo testo con il length filter */
            if (math.abs(docLen - neighbor.docLength) <= threshold) {
              /** Lo posso aggiungere solo se le loro posizioni non sono distanti più della soglia
                * altrimenti non posso dire niente. Non lo posso escludere ancora però,
                * perché potrebbero avere una posizione abbastanza vicina in qualche altro blocco.
                * */
              if (math.abs(qgramPos - neighbor.qgramPos) <= threshold) {
                neighbors.update(numneighbors, (nId, 0))
                numneighbors += 1
              }
            }
            else {
              /** Se non lo passa lo droppo */
              cbs.update(nId, -1)
              excluded.update(numExcluded, nId)
              numExcluded += 1
            }
          }

          /**
            * Devo verificare che sia valido, potrei averlo annullato con il length filter
            * Inoltre perché sia valido, oltre ad appartenere allo stesso blocco, deve anche essere
            * sufficientemente vicino (positional filter).
            **/
          if (cbs(nId) >= 0 && math.abs(qgramPos - neighbor.qgramPos) <= threshold) {
            /** Metto a +1 le volte in cui l'ho visto (alla fine ottengo i q-grammi comuni nel prefisso) */
            cbs.update(nId, cbs(nId) + 1)

            /** Salvo l'ultima posizione in cui l'ho visto */
            if (pos(nId) < neighbor.sortedPos) {
              pos.update(nId, neighbor.sortedPos)
            }

            /** Salvo la mia posizione in cui l'ho visto (di sicuro è sempre l'ultima) */
            myPos.update(nId, i + 1)
          }
        }
      }
    }
    (docLen, numneighbors, numExcluded)
  }

  /**
    * Esegue il calcolo per la Jaccard Similarity
    **/
  def computeJS(tokens: Array[Int],
                attribute: String,
                threshold: Double,
                prefixes: Map[String, Map[Int, List[PrefixEntry]]],
                docId: Int,
                cbs: Array[Int],
                neighbors: Array[(Long, Int)],
                numneighborsCur: Int,
                numExcludedCur: Int,
                excluded: Array[Int],
                pos: Array[Int],
                myPos: Array[Int]
               ): (Int, Int, Int) = {
    var numneighbors = numneighborsCur
    var numExcluded = numExcludedCur
    /** Calcola la lunghezza dell'attributo (numero di token) */
    val docLen = tokens.length
    /** Legge il prefix-index per questo attributo */
    val prefixLen = JsFilters.getPrefixLength(docLen, threshold)

    /** Per ogni token all'interno del prefisso */
    for (i <- 0 until prefixLen) {
      /** Legge dall'indice (se esiste) il blocco corrispondente a questo token */
      val block = prefixes(attribute).get(tokens(i))
      if (block.isDefined) {
        /**
          * Prende i vicini che:
          *  - Hanno Id < del documento attuale (evito duplicati)
          *  - Sono ancora validi, ossia hanno passato uno step precedente cbs >= 0
          **/
        val validneighbors = block.get.filter(n => docId < n.docId && cbs(n.docId.toInt) >= 0)

        /** Per ogni vicino */
        validneighbors.foreach { neighbor =>
          val nId = neighbor.docId.toInt

          /** Se il CBS è 0 è la prima volta che lo trovo */
          if (cbs(nId) == 0) {
            /** Essendo la prima volta che lo incontro lo testo con il length filter */
            if (Math.min(docLen, neighbor.docLen) >= Math.min(docLen, neighbor.docLen) * threshold) {
              neighbors.update(numneighbors, (nId, neighbor.docLen))
              numneighbors += 1
            }
            else {
              /** Se non lo passa lo droppo */
              cbs.update(nId, -1)
              excluded.update(numExcluded, nId)
              numExcluded += 1
            }
          }

          /**
            * Devo verificare che sia valido, potrei averlo annullato con il length filter
            **/
          if (cbs(nId) >= 0) {
            /** Salvo l'ultima posizione in cui l'ho visto */
            if (pos(nId) < neighbor.tokenPos) {
              pos.update(nId, neighbor.tokenPos)
            }

            /** Salvo la mia posizione in cui l'ho visto (di sicuro è sempre l'ultima) */
            myPos.update(nId, i + 1)

            /** Metto a +1 le volte in cui l'ho visto (alla fine ottengo i token comuni nel prefisso) */
            cbs.update(nId, cbs(nId) + 1)
          }
        }
      }
    }

    (docLen, numneighbors, numExcluded)
  }

  def preprocessing(profiles: List[Profile], attributesThresholds: Map[String, (Double, String)], log: PrintWriter, sortType: String, qgramLength: Int = 2): (Map[Long, Array[(String, tokenized)]], Map[String, Map[Int, List[PrefixEntry]]], Map[String, Map[Int, List[Qgram]]], Map[Long, Map[String, tokenized]]) = {
    val t1 = Calendar.getInstance().getTimeInMillis

    /**
      * Per ogni attributo genera i token nel caso in cui debba fare la jaccard similarity, oppure i q-grammi
      * se deve fare l'ED.
      * Quindi alla fine si avrà una struttura che contiene:
      * Attributo -> (tokens, prefixIndex)
      **/
    val data: Map[String, docData] = attributesThresholds.map { case (attribute, (threshold, thresholdType)) =>
      val docs = CommonFunctions.extractField(profiles, attribute)
      if (thresholdType == thresholdTypes.JS) {
        //Jaccard
        val tokenizedDocs: List[(Long, Array[Int])] = CommonJsFunctions.tokenizeAndSort(docs).filter(_._2.length > 0)
        (attribute, docJsData(attribute, CommonJsFunctions.buildPrefixIndex(tokenizedDocs, threshold), tokenizedDocs))
      }
      else {
        //ED
        val qgrams = docs.map(d => (d._1, d._2, CommonEdFunctions.getQgrams(d._2, qgramLength)))
        val sortedQgrams = CommonEdFunctions.getSortedQgrams2(qgrams)
        (attribute, docEdData(attribute, CommonEdFunctions.buildPrefixIndex(sortedQgrams.map(x => (x._1, x._3)), qgramLength, threshold.toInt), sortedQgrams))
      }
    }

    /**
      * Per ogni documento genera una mappa che contiene per ogni attributo (su cui c'è una condizione) i suoi
      * qgrammi/tokens.
      * Quindi la mappa sarà:
      * Documento -> attributo1 -> tokens/q-grammi dell'attributo1
      * attributo2 -> tokens/q-grammi dell'attributo2
      * ...
      **/
    val docTokens: Map[Long, Map[String, tokenized]] = data
      .flatMap { case (attribute, docData) =>
        if (docData.getType() == thresholdTypes.JS) {
          val jsData = docData.asInstanceOf[docJsData]
          jsData.tokenizedDocs.map { case (docId, tokens) => (docId, attribute, tokensJs(tokens)) }
        }
        else {
          val edData = docData.asInstanceOf[docEdData]
          edData.qgrams.map { case (docId, str, qgrams1) => (docId, attribute, tokensED(str, qgrams1)) }
        }
      }
      .groupBy { case (docId, attribute, tokens) => docId }
      .filter { case (docId, tokensPerAttribute) => tokensPerAttribute.size == attributesThresholds.size }
      .map { case (docId, tokensPerAttribute) =>
        val attributeTokensMap = tokensPerAttribute.map { case (docId1, attribute, tokens) =>
          (attribute, tokens)
        }.toMap
        (docId, attributeTokensMap)
      }

    val prefixIndexJS: Map[String, Map[Int, List[PrefixEntry]]] = data.map { case (attribute, indexData) =>
      if (indexData.getType() == thresholdTypes.JS) {
        (attribute, indexData.asInstanceOf[docJsData].prefixIndex)
      }
      else {
        (attribute, Map.empty[Int, List[PrefixEntry]])
      }
    }.filter(_._2.nonEmpty)

    val prefixIndexED: Map[String, Map[Int, List[Qgram]]] = data.map { case (attribute, indexData) =>
      if (indexData.getType() == thresholdTypes.ED) {
        (attribute, indexData.asInstanceOf[docEdData].prefixIndex)
      }
      else {
        (attribute, Map.empty[Int, List[Qgram]])
      }
    }.filter(_._2.nonEmpty)

    val t2 = Calendar.getInstance().getTimeInMillis
    log.println("[GraphJoin] Tempo preprocessing " + CommonFunctions.msToMin(t2 - t1))

    /**
      * Calcola delle statistiche sui blocchi.
      * In particolare per ogni attributo viene calcolata la dimensione media del blocco
      */
    case class Stats(numPrefixDocs: Double, numPrefixTokens: Double, simType: Int, attrEntropy: Double)

    val sortEntro = List(sortTypes.entroDesc, sortTypes.entroAsc)
    val sortAvg = List(sortTypes.avgBlockSizeDesc, sortTypes.avgBlockSizeAsc)

    val stats = data.map { case (attribute, docData) =>
      if (docData.getType() == thresholdTypes.JS) {
        val jsData = docData.asInstanceOf[docJsData]

        val entropy = {
          if (sortEntro.contains(sortType)) {
            CommonJsFunctions.calcEntropy(jsData.tokenizedDocs)
          }
          else {
            0
          }
        }

        val (docNum, prefixTokens) = {
          if (sortAvg.contains(sortType)) {
            val s = jsData.prefixIndex.toArray.map(_._2.length.toDouble)
            (s.length.toDouble, s.sum)
          }
          else {
            (0.0, 0.0)
          }
        }

        (attribute, Stats(docNum, prefixTokens, 0, entropy))
      }
      else {
        val edData = docData.asInstanceOf[docEdData]
        val entropy = {
          if (sortEntro.contains(sortType)) {
            CommonEdFunctions.calcEntropy(edData.qgrams)
          }
          else {
            0
          }
        }

        val (docNum, prefixTokens) = {
          if (sortAvg.contains(sortType)) {
            val s = edData.prefixIndex.toArray.map(_._2.size.toDouble)
            (s.length.toDouble, s.sum)
          }
          else {
            (0.0, 0.0)
          }
        }

        (attribute, Stats(docNum, prefixTokens, 1, entropy))
      }
    }
    log.println("[GraphJoin] Statistiche sui blocchi")
    log.println(stats)
    val t3 = Calendar.getInstance().getTimeInMillis

    log.println("[GraphJoin] Tempo calcolo statistiche blocchi " + CommonFunctions.msToMin(t3 - t2))


    /**
      * Per ogni documento contiene i token di ogni attributo su cui sono applicate delle condizioni
      **/
    val sortedTokensPerDocument = docTokens.map { case (docId, indexes) =>
      val i = indexes.toArray
      val sorted = {
        if (sortType == sortTypes.avgBlockSizeAsc) {
          i.sortBy(x => (stats(x._1).simType, stats(x._1).numPrefixTokens / stats(x._1).numPrefixDocs))
        }
        else if (sortType == sortTypes.avgBlockSizeDesc) {
          i.sortBy(x => (stats(x._1).simType, -stats(x._1).numPrefixTokens / stats(x._1).numPrefixDocs))
        }
        else if (sortType == sortTypes.thresholdAsc) {
          i.sortBy(x => (stats(x._1).simType, attributesThresholds(x._1)._1))
        }
        else if (sortType == sortTypes.thresholdDesc) {
          i.sortBy(x => (stats(x._1).simType, -attributesThresholds(x._1)._1))
        }
        else if (sortType == sortTypes.entroAsc) {
          i.sortBy(x => (stats(x._1).simType, stats(x._1).attrEntropy))
        }
        else {
          i.sortBy(x => (stats(x._1).simType, -stats(x._1).attrEntropy))
        }
      }
      (docId, sorted)
    }

    val t4 = Calendar.getInstance().getTimeInMillis

    log.println("[GraphJoin] Tempo ordinamento " + CommonFunctions.msToMin(t4 - t3))

    log.println("[GraphJoin] Record ordinato")
    log.println(sortedTokensPerDocument.take(1).map(x => x._2.toList))
    log.flush()

    (sortedTokensPerDocument, prefixIndexJS, prefixIndexED, docTokens)
  }

  def verify(candidates: List[(Long, Long)], docTokens: Map[Long, Map[String, tokenized]], attributesThresholds: Map[String, (Double, String)]): List[(Long, Long)] = {
    def check(doc1: Array[Int], doc2: Array[Int], threshold: Double): Boolean = {
      val common = doc1.intersect(doc2).length
      (common.toDouble / (doc1.length + doc2.length - common)) >= threshold
    }


    candidates.filter { case (doc1, doc2) =>
      val d1 = docTokens.get(doc1)
      val d2 = docTokens.get(doc2)
      var pass: Boolean = true
      if (d1.isDefined && d2.isDefined) {
        val docs1 = d1.get
        val docs2 = d2.get
        val it = attributesThresholds.iterator
        while (it.hasNext && pass) {
          val (attribute, threshold) = it.next()

          if (threshold._2 == thresholdTypes.JS) {
            //Controllo la jaccard
            pass = check(docs1(attribute).asInstanceOf[tokensJs].tokens, docs2(attribute).asInstanceOf[tokensJs].tokens, threshold._1)
          }
          else {
            //Controllo con l'edit distance
            pass = CommonEdFunctions.editDist(docs1(attribute).asInstanceOf[tokensED].originalStr, docs2(attribute).asInstanceOf[tokensED].originalStr) <= threshold._1
          }
        }
      }
      else {
        pass = false
      }
      pass
    }
  }


  /**
    * Calcola i match ponendo le condizioni in AND tra di loro
    **/
  def getMatchesMultiSort(profiles: List[Profile], attributesThresholds: Map[String, (Double, String)], log: PrintWriter, sortType: String, qgramLength: Int = 2): List[(Long, Long)] = {

    val (sortedTokensPerDocument, prefixIndexJS, prefixIndexED, docTokens) = preprocessing(profiles, attributesThresholds, log, sortType, qgramLength)

    val t4 = Calendar.getInstance().getTimeInMillis

    val maxId = profiles.maxBy(_.id).id.toInt + 1

    /** Numero di comparison che fa */
    var comparisonNumber: Double = 0

    /** Numero di common filter */
    var commonNumber: Double = 0

    /** Elenco di candidati */
    var candidates: List[(Long, Long)] = Nil

    /** Lunghezza documento (usato nella JS) */
    var docLen = 0
    /** Numero record esclusi */
    var numExcluded = 0
    /** Record esclusi */
    val excluded: Array[Int] = Array.ofDim(maxId)
    /** Record vicini */
    val neighbors = Array.ofDim[(Long, Int)](maxId)
    /** Posizione in cui il record esterno ha matchato con un vicino */
    val myPos = Array.ofDim[Int](maxId)
    /** Posizione in cui ho trovato il vicino */
    val pos = Array.ofDim[Int](maxId)
    /** Numero di blocchi in comune con un certo vicino, in pratica è il numero di token */
    val cbs = Array.fill[Int](maxId) {
      0
    }
    /** Dice che la condizione è la prima */
    var isFirst = true
    /** Numero di vicini */
    var numneighbors = 0

    /** Primi vicini di ogni giro */
    val firstneighbors = Array.ofDim[Int](maxId)
    var firstneighborsNum = 0

    val t5 = Calendar.getInstance().getTimeInMillis

    log.println("[GraphJoin] Tempo inizializzazione " + CommonFunctions.msToMin(t5 - t4))

    /** Per ogni documento */
    sortedTokensPerDocument.foreach { case (docId, attributesTokens) =>

      /** Dico che l'attributo è il primo */
      isFirst = true

      /** Per ogni attributo all'interno del documento */
      attributesTokens.foreach { case (attribute, tokens) =>

        /** Legge la soglia e il tipo di soglia da applicare a questo attributo */
        val (threshold, thresholdType) = attributesThresholds(attribute)

        /** Calcola la Jaccard Similarity */
        if (thresholdType == thresholdTypes.JS) {
          computeJS(
            tokens.asInstanceOf[tokensJs].tokens,
            attribute,
            threshold,
            prefixIndexJS,
            docId.toInt,
            cbs,
            neighbors,
            numneighbors,
            numExcluded,
            excluded,
            pos,
            myPos
          )
          match {
            case (dLen, nneighbors, nExcl) =>
              docLen = dLen
              numneighbors = nneighbors
              numExcluded = nExcl
          }
        }

        /** Calcola l'Edit Distance */
        else {
          computeED(
            tokens.asInstanceOf[tokensED].qgrams,
            qgramLength,
            threshold.toInt,
            prefixIndexED,
            attribute,
            docId,
            cbs,
            neighbors,
            numneighbors,
            numExcluded,
            excluded,
            pos,
            myPos
          )
          match {
            case (dLen, nneighbors, nExcl) =>
              docLen = dLen
              numneighbors = nneighbors
              numExcluded = nExcl
          }
        }


        /** Se alla fine del processo ci sono dei vicini che erano nel primo attributo che non sono stati trovati
          * in questo attributo, devono essere annullati.
          * Se non sono stati trovati hanno il cbs a 0
          * */
        for (i <- 0 until firstneighborsNum if cbs(firstneighbors(i)) == 0) {
          cbs.update(firstneighbors(i), -1)
          excluded.update(numExcluded, firstneighbors(i))
          numExcluded += 1
        }

        /** Scorre i vicini validi */
        for (i <- 0 until numneighbors) {
          val nId = neighbors(i)._1.toInt
          comparisonNumber += 1
          if (thresholdType == thresholdTypes.JS) {
            /** Li testa con il position filter, se lo passano li aggiunge tra quelli mantenuti, altrimenti li droppa */
            if (JsFilters.positionFilter(docLen, neighbors(i)._2, myPos(nId), pos(nId), cbs(nId), threshold)) {
              /** Lo mantengo, l'aggiunta a fistneighbors la fa solo il primo attributo, gli altri non ha senso che la facciano,
                * al massimo rimuovono! Quindi questi poi li verificherò alla fine che non abbiano il cbs settato a -1 */
              if (isFirst) {
                firstneighbors.update(firstneighborsNum, nId)
                firstneighborsNum += 1
              }
              cbs.update(nId, 0)
            }
            else {
              /** Droppo il vicino */
              cbs.update(nId, -1)
              excluded.update(numExcluded, nId)
              numExcluded += 1
            }
          }
          else {
            commonNumber += 1
            if (docTokens.contains(nId) && EdFilters.commonFilterAfterPrefix(tokens.asInstanceOf[tokensED].qgrams, docTokens(nId)(attribute).asInstanceOf[tokensED].qgrams, qgramLength, threshold.toInt, cbs(nId), myPos(nId), pos(nId))) {
              /** Lo mantengo, l'aggiunta a fistneighbors la fa solo il primo attributo, gli altri non ha senso che la facciano,
                * al massimo rimuovono! Quindi questi poi li verificherò alla fine che non abbiano il cbs settato a -1 */
              if (isFirst) {
                firstneighbors.update(firstneighborsNum, nId)
                firstneighborsNum += 1
              }
              cbs.update(nId, 0)
            }
            else {
              /** Droppo il vicino */
              cbs.update(nId, -1)
              excluded.update(numExcluded, nId)
              numExcluded += 1
            }
          }
          pos.update(nId, 0)
        }

        /** Resetto il numero di vicini per il prossimo giro */
        numneighbors = 0

        /** Dice che ha finito di fare il primo attributo */
        isFirst = false
      }

      /** Emetto le coppie candidate relative a questo profilo */
      for (i <- 0 until firstneighborsNum if cbs(firstneighbors(i)) >= 0) {
        candidates = (docId, firstneighbors(i).toLong) :: candidates
      }
      firstneighborsNum = 0

      /** Alla fine di tutti gli attributi, resetto tutto per il prossimo documento */
      for (i <- 0 until numExcluded) {
        cbs.update(excluded(i), 0)
      }
      numExcluded = 0
    }

    log.println("[GraphJoin] Numero di volte che è stato applicato il position filter " + comparisonNumber)
    log.println("[GraphJoin] Numero di volte che è stato applicato il common filter " + commonNumber)
    log.println("[GraphJoin] Numero di candidati " + candidates.length)

    val t6 = Calendar.getInstance().getTimeInMillis

    log.println("[GraphJoin] Tempo di join (no preprocessing) " + CommonFunctions.msToMin(t6 - t5))

    val pairs = verify(candidates, docTokens, attributesThresholds)

    log.println("[GraphJoin] Numero di record verificati " + pairs.length)

    val t7 = Calendar.getInstance().getTimeInMillis

    log.println("[GraphJoin] Tempo di verifica " + CommonFunctions.msToMin(t7 - t6))

    log.println(attributesThresholds + ";" + sortType + ";" + comparisonNumber + ";" + candidates.length + ";" + pairs.length)

    log.flush()

    pairs
  }

}
