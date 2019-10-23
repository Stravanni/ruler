package SimJoins.SimJoinsSingleNode.SimJoins

import java.io.PrintWriter
import java.util.Calendar

import SimJoins.SimJoinsSingleNode.Commons.CommonFunctions
import SimJoins.SimJoinsSingleNode.Commons.ED.{CommonEdFunctions, EdFilters}
import SimJoins.SimJoinsSingleNode.Commons.ED.CommonEdFunctions.commons
import SimJoins.SimJoinsSingleNode.Commons.JS.CommonJsFunctions
import SimJoins.SimJoinsSingleNode.Commons.JS.JsFilters
import SimJoins.DataStructure.{PrefixEntry, Profile, Qgram}

/**
  * @author Luca Gagliardelli
  * @since 23/20/2019
  * */
object GraphJoinOrAnd {

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

  case class docJsData(attribute: String, prefixIndex: Map[Double, Map[Int, List[PrefixEntry]]], tokenizedDocs: List[(Long, Array[Int])]) extends docData {
    def getType(): String = thresholdTypes.JS
  }

  case class docEdData(attribute: String, prefixIndex: Map[Int, Map[Int, List[Qgram]]], qgrams: List[(Long, String, Array[(Int, Int)])]) extends docData {
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

  /**
    * Prende in ingresso le condizioni in OR tra di loro
    * e le formatta in modo da averle in un'unica struttura nel formato:
    * attributo -> tipo condizione (ED/JS) -> [soglie]
    **/
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
                 prefixIndexED: Map[Int, List[Qgram]],
                 docId: Long,
                 cbs: Array[Int],
                 neighbors: Array[(Long, Int)],
                 numneighborsExt: Int,
                 numExcludedExt: Int,
                 excluded: Array[Int],
                 pos: Array[Int],
                 myPos: Array[Int],
                 visited: Array[Boolean]
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
      val block = prefixIndexED.get(qgram)
      if (block.isDefined) {
        val validneighbors = block.get.filter(n => docId < n.docId && cbs(n.docId.toInt) >= 0)

        /** Per ogni vicino valido */
        validneighbors.foreach { neighbor =>
          val nId = neighbor.docId.toInt

          if (!visited(nId)) {
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
    }
    (docLen, numneighbors, numExcluded)
  }

  /**
    * Esegue il calcolo per la Jaccard Similarity
    **/
  def computeJS(tokens: Array[Int],
                threshold: Double,
                prefixes: Map[Int, List[PrefixEntry]],
                docId: Int,
                cbs: Array[Int],
                neighbors: Array[(Long, Int)],
                numneighborsCur: Int,
                numExcludedCur: Int,
                excluded: Array[Int],
                pos: Array[Int],
                myPos: Array[Int],
                visited: Array[Boolean]
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
      val block = prefixes.get(tokens(i))
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

          if (!visited(nId)) {

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
    }

    (docLen, numneighbors, numExcluded)
  }


  /**
    * Esegue il pre-processing per la generazione degli indici
    **/
  def preprocessing(profiles: List[Profile],
                    conditionsOR: List[Map[String, (Double, String)]],
                    log: PrintWriter,
                    sortType: String,
                    qgramLength: Int = 2): (Map[Long, Map[String, Map[String, tokenized]]], Map[String, Map[Double, Map[Int, List[PrefixEntry]]]], Map[String, Map[Int, Map[Int, List[Qgram]]]], Map[Long, Map[String, Map[String, tokenized]]]) = {
    val t1 = Calendar.getInstance().getTimeInMillis

    /**
      * Formatta le condizioni in AND tra loro, in modo da averle in un'unica lista
      **/
    val conditionsPerAttribute = parseConditions(conditionsOR)

    /**
      * Per ogni attributo genera i token nel caso in cui debba fare la jaccard similarity, oppure i q-grammi
      * se deve fare l'ED.
      * Quindi alla fine si avrà una struttura che contiene:
      * Attributo -> Tipo condizione -> (tokens, prefixIndex)
      * Perché su un attributo si potrebbe applicare sia ED che JS, quindi ho una mappa su 3 livelli:
      * attributo -> condizione -> (tokens, prefix index per soglie)
      **/
    val data = conditionsPerAttribute.flatMap { case (attribute, conditions) =>
      val docs = CommonFunctions.extractField(profiles, attribute)
      conditions.map { case (conditionType, thresholds) =>
        if (conditionType == thresholdTypes.JS) {
          //Jaccard
          val tokenizedDocs: List[(Long, Array[Int])] = CommonJsFunctions.tokenizeAndSort(docs).filter(_._2.length > 0)

          val prefixes = thresholds.map { threshold =>
            (threshold, CommonJsFunctions.buildPrefixIndex(tokenizedDocs, threshold))
          }

          (attribute, conditionType, docJsData(attribute, prefixes.toMap, tokenizedDocs))
        }
        else {
          //ED
          val qgrams = docs.map(d => (d._1, d._2, CommonEdFunctions.getQgrams(d._2, qgramLength)))
          val sortedQgrams = CommonEdFunctions.getSortedQgrams2(qgrams)

          val prefixes = thresholds.map { threshold =>
            (threshold.toInt, CommonEdFunctions.buildPrefixIndex(sortedQgrams.map(x => (x._1, x._3)), qgramLength, threshold.toInt))
          }
          (attribute, conditionType, docEdData(attribute, prefixes.toMap, sortedQgrams))
        }
      }
    }.groupBy(_._1).map { x =>
      (x._1, x._2.map(y => (y._2, y._3)).toMap)
    }

    /**
      * Per ogni documento genera una mappa che contiene per ogni attributo (su cui c'è una condizione) i suoi
      * qgrammi/tokens.
      * Quindi la mappa sarà:
      * Documento -> attributo1 -> tipo di soglia (JS/ED) -> tokens/q-grammi dell'attributo1
      * ...
      **/
    val docTokens: Map[Long, Map[String, Map[String, tokenized]]] = data
      .flatMap { case (attribute, docDatas) =>
        docDatas.flatMap { case (thresholdType, docData) =>
          if (thresholdType == thresholdTypes.JS) {
            val jsData = docData.asInstanceOf[docJsData]
            jsData.tokenizedDocs.map { case (docId, tokens) => (docId, attribute, docData.getType(), tokensJs(tokens)) }
          }
          else {
            val edData = docData.asInstanceOf[docEdData]
            edData.qgrams.map { case (docId, str, qgrams1) => (docId, attribute, docData.getType(), tokensED(str, qgrams1)) }
          }
        }
      }
      .groupBy { case (docId, attribute, thresholdType, tokens) => docId }
      //.filter { case (docId, tokensPerAttribute) => tokensPerAttribute.size == attributesThresholds.size }
      .map { case (docId, tokensPerAttribute) =>
        val attributeTokensMap = tokensPerAttribute.toArray.map { case (docId1, attribute, thresholdType, tokens) =>
          (attribute, (thresholdType, tokens))
        }.groupBy(_._1).map { x =>
          (x._1, x._2.map(y => y._2).toMap)
        }
        (docId, attributeTokensMap)
      }

    /**
      * Contiene per ogni attributo, soglia e documento, l'elenco dei prefissi
      **/
    val prefixIndexJS: Map[String, Map[Double, Map[Int, List[PrefixEntry]]]] = data.map { case (attribute, indexData) =>
      val a = indexData.get(thresholdTypes.JS)
      if (a.isDefined) {
        (attribute, a.get.asInstanceOf[docJsData].prefixIndex)
      }
      else {
        (attribute, Map.empty[Double, Map[Int, List[PrefixEntry]]])
      }
    }.filter(_._2.nonEmpty)

    /**
      * Contiene per ogni attributo, soglia e documento, l'elenco dei prefissi
      **/
    val prefixIndexED: Map[String, Map[Int, Map[Int, List[Qgram]]]] = data.map { case (attribute, indexData) =>

      val a = indexData.get(thresholdTypes.ED)
      if (a.isDefined) {
        (attribute, a.get.asInstanceOf[docEdData].prefixIndex)
      }
      else {
        (attribute, Map.empty[Int, Map[Int, List[Qgram]]])
      }
    }.filter(_._2.nonEmpty)

    val sortedTokensPerDocument = docTokens

    (sortedTokensPerDocument, prefixIndexJS, prefixIndexED, docTokens)
  }

  /**
    * Verifica i candidati, mantenendo solo quelli che soddisfano le condizioni
    **/
  def verify(candidates: List[(Long, Long)],
             docTokens: Map[Long, Map[String, Map[String, tokenized]]],
             conditionsOR: List[Map[String, (Double, String)]]): List[(Long, Long)] = {
    /**
      * Dati due documenti e la soglia, verifica che la passino
      **/
    def passJS(doc1: Array[Int], doc2: Array[Int], threshold: Double): Boolean = {
      val common = doc1.intersect(doc2).length
      (common.toDouble / (doc1.length + doc2.length - common)) >= threshold
    }

    candidates.filter { case (doc1, doc2) =>
      val d1 = docTokens.get(doc1)
      val d2 = docTokens.get(doc2)
      var noPassOR = true
      var passAND: Boolean = true

      if (d1.isDefined && d2.isDefined) {
        val docs1 = d1.get
        val docs2 = d2.get

        val conditionsORIt = conditionsOR.iterator

        /**
          * Continua o finché non sono finite le condizioni in OR
          * o finché uno dei blocchi in AND non dà esito positivo, se uno dei blocchi
          * AND dà esito positivo è inutile provare gli altri.
          **/
        while (conditionsORIt.hasNext && noPassOR) {
          val conditionsAND = conditionsORIt.next()
          val conditionsANDIt = conditionsAND.iterator
          passAND = true

          /**
            * Continua finché passa tutte le singole condizioni in AND tra loro, o finché non sono terminate
            **/
          while (conditionsANDIt.hasNext && passAND) {
            val (attribute, threshold) = conditionsANDIt.next()
            if (docs1.contains(attribute) && docs2.contains(attribute)) {
              if (docs1(attribute).contains(threshold._2) && docs2(attribute).contains(threshold._2)) {
                if (threshold._2 == thresholdTypes.JS) {
                  //Controllo la jaccard
                  passAND = passJS(docs1(attribute)(threshold._2).asInstanceOf[tokensJs].tokens, docs2(attribute)(threshold._2).asInstanceOf[tokensJs].tokens, threshold._1)
                }
                else {
                  //Controllo con l'edit distance
                  passAND = CommonEdFunctions.editDist(docs1(attribute)(threshold._2).asInstanceOf[tokensED].originalStr, docs2(attribute)(threshold._2).asInstanceOf[tokensED].originalStr) <= threshold._1
                }
              }
            }
          }

          /**
            * Se ha terminato il ciclo perché ha finito (passAND = true), allora fermo il processo, le altre condizioni non devono essere verificate.
            * Altrimenti continua.
            **/
          noPassOR = !passAND
        }
      }
      else {
        noPassOR = true
      }
      !noPassOR
    }
  }

  def getMatchesMultiSort(profiles: List[Profile],
                          conditionsOR: List[Map[String, (Double, String)]],
                          log: PrintWriter, sortType: String,
                          qgramLength: Int = 2): List[(Long, Long)] = {

    /**
      * Esegue il pre-processing per creare tutti gli indici che servono per il processo di join
      **/
    val (sortedTokensPerDocument, prefixIndexJS, prefixIndexED, docTokens) = preprocessing(profiles, conditionsOR, log, sortType, qgramLength)

    val t4 = Calendar.getInstance().getTimeInMillis

    /** Elenco di candidati */
    var candidates: List[(Long, Long)] = Nil

    /** --------------------- VARIABILI A FINI STATISTICI PER I TEST ------------------------ */

    /** Numero di volte che viene eseguito il position filter */
    var positionNumber: Double = 0

    /** Numero di volte che viene eseguito il common filter */
    var commonNumber: Double = 0

    /** --------------------- VARIABILI GENERICHE ------------------------ */
    /** Id massimo dei profili */
    val maxId = profiles.maxBy(_.id).id.toInt + 1


    /** --------------------- VARIABILI PER ESEGUIRE L'OR ------------------------ */

    /** Nel processo di OR indica i vicini che sono già stati visti */
    val visited = Array.fill[Boolean](maxId) {
      false
    }
    /** Numero di record emessi per ogni profilo */
    var numEmitted = 0
    /** Record emessi per ogni profilo, serve per evitarne la computazione quando si fanno le condizioni di AND */
    val emitted = Array.ofDim[Int](maxId)

    /** --------------------- VARIABILI PER ESEGUIRE L'AND ------------------------ */

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
    /** Serve ad indicare che si sta eseguendo la prima condizione */
    var isFirst = true
    /** Numero di vicini */
    var numneighbors = 0
    /** Primi vicini validi trovati alla prima condizione */
    val firstneighbors = Array.ofDim[Int](maxId)
    /** Numero dei primi vicini validi trovati */
    var firstneighborsNum = 0

    val t5 = Calendar.getInstance().getTimeInMillis

    log.println("[GraphJoin] Tempo inizializzazione " + CommonFunctions.msToMin(t5 - t4))

    /** Per ogni documento presente */
    sortedTokensPerDocument.foreach { case (docId, attributesTokens) =>

      /** Per ogni set di condizioni in OR tra di loro */
      conditionsOR.foreach { conditionsAND =>

        /** Segno che è la prima condizione tra quelle in AND che viene eseguita */
        isFirst = true

        /** Per ogni condizione in questo blocco AND calcolo i vicini validi */
        conditionsAND.foreach { case (attribute, (threshold, thresholdType)) =>

          /** Controllo che il record corrente abbia l'attributo su cui applicare la condizione e ottengo i token */
          val attributeIndex = attributesTokens.get(attribute)
          if (attributeIndex.isDefined) {
            val tokenIndex = attributeIndex.get.get(thresholdType)
            if (tokenIndex.isDefined) {
              val tokens = tokenIndex.get

              /** Calcola la Jaccard Similarity, vuole dire che la condizione da applicare è una JS */
              if (thresholdType == thresholdTypes.JS) {
                computeJS(
                  tokens.asInstanceOf[tokensJs].tokens,
                  threshold,
                  prefixIndexJS(attribute)(threshold),
                  docId.toInt,
                  cbs,
                  neighbors,
                  numneighbors,
                  numExcluded,
                  excluded,
                  pos,
                  myPos,
                  visited
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
                  prefixIndexED(attribute)(threshold.toInt),
                  docId,
                  cbs,
                  neighbors,
                  numneighbors,
                  numExcluded,
                  excluded,
                  pos,
                  myPos,
                  visited
                )
                match {
                  case (dLen, nneighbors, nExcl) =>
                    docLen = dLen
                    numneighbors = nneighbors
                    numExcluded = nExcl
                }
              }


              /**
                * Se alla fine del processo ci sono dei vicini che erano nel primo attributo che non sono stati trovati
                * in questo attributo, devono essere annullati.
                * Se non sono stati trovati hanno il cbs a 0
                **/
              for (i <- 0 until firstneighborsNum if cbs(firstneighbors(i)) == 0) {
                cbs.update(firstneighbors(i), -1)
                excluded.update(numExcluded, firstneighbors(i))
                numExcluded += 1
              }

              /** Scorre i vicini validi */
              for (i <- 0 until numneighbors) {
                val nId = neighbors(i)._1.toInt
                positionNumber += 1
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
                  if (docTokens.contains(nId) && EdFilters.commonFilterAfterPrefix(tokens.asInstanceOf[tokensED].qgrams, docTokens(nId)(attribute)(thresholdTypes.ED).asInstanceOf[tokensED].qgrams, qgramLength, threshold.toInt, cbs(nId), myPos(nId), pos(nId))) {
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
          }
        }

        /** Emetto le coppie candidate relative a questo profilo */
        for (i <- 0 until firstneighborsNum if cbs(firstneighbors(i)) >= 0) {
          candidates = (docId, firstneighbors(i).toLong) :: candidates
          emitted.update(numEmitted, firstneighbors(i))
          numEmitted += 1

          /** Segno che il profilo è stato visitato, in questo modo se un succesivo blocco AND lo trova, non lo fa */
          visited.update(firstneighbors(i), true)
        }

        /** Alla fine di tutti gli attributi, resetto tutto per il prossimo documento */
        for (i <- 0 until numExcluded) {
          cbs.update(excluded(i), 0)
        }
        numExcluded = 0
        firstneighborsNum = 0
      }

      /** Alla fine del documento, sono state eseguite tutte le condizioni in OR, resetto i dati sui profili visti */
      for (i <- 0 until numEmitted) {
        visited.update(emitted(i), false)
      }
      numEmitted = 0
    }

    log.println("[GraphJoin] Numero di volte che è stato applicato il position filter " + positionNumber)
    log.println("[GraphJoin] Numero di volte che è stato applicato il common filter " + commonNumber)
    log.println("[GraphJoin] Numero di candidati " + candidates.length)

    val t6 = Calendar.getInstance().getTimeInMillis

    log.println("[GraphJoin] Tempo di join (no preprocessing) " + CommonFunctions.msToMin(t6 - t5))

    val pairs = verify(candidates, docTokens, conditionsOR)

    log.println("[GraphJoin] Numero di record verificati " + pairs.length)

    val t7 = Calendar.getInstance().getTimeInMillis

    log.println("[GraphJoin] Tempo di verifica " + CommonFunctions.msToMin(t7 - t6))

    log.println(conditionsOR + ";" + sortType + ";" + positionNumber + ";" + candidates.length + ";" + pairs.length)

    log.flush()

    pairs
  }
}
