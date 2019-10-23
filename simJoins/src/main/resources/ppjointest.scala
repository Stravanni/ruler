import java.util.Calendar

import BlockBuildingMethods.{BlockingUtils, LSHMio}
import DataPreparation.PrefixDataBuilder
import DataStructures.{KeysCluster, Profile}
import JoinEngine.ERJoin
import Utilities.ER
import Wrappers.{CSVWrapper, JSONWrapper, SerializedObjectLoader}
import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import utils.Tokenizer

/**
  * Created by Luca on 06/03/2018.
  */

object WRAPPER_TYPES {
  val serialized = "serialized"
  val csv = "csv"
  val json = "json"
}

def msToMin(ms: Long): Double = (ms / 1000.0) / 60.0

def PPJoinFn(profiles1 : RDD[(Long, String)], profiles2 : RDD[(Long, String)], separatorID : Long, threshold : Double) : RDD[(Long, Long)] = {
  val log: Logger = LogManager.getRootLogger
  log.info("SPARKER - Genero profili")
  val data = profiles1.union(profiles2)
  data.cache()
  data.count()
  log.info("SPARKER - Fatto")
  log.info("SPARKER - Tokenizzazione")
  val tokenizedData = PrefixDataBuilder.prepareAllTokens(data, Tokenizer.Word)
  tokenizedData.cache()
  tokenizedData.count()
  data.unpersist()
  log.info("SPARKER - Fatta")
  log.info("SPARKER - Esegue PPJOIN")
  //val candidates = PPJoin.rddJoinVerifyADBalanced(tokenizedData, separatorID, threshold)
  //val candidates = TTJoin.joinAllTokensVerifyBalancing(tokenizedData, threshold, separatorID)(JaccardOrdered.verify)
  val candidates = ERJoin.rddJoinVerifyBalanced(tokenizedData, separatorID, threshold)
  candidates.cache()
  candidates.count()
  log.info("FATTO")
  //candidates.map(x => (x._1._1, x._2._1))
  candidates
}

def getTokens(profiles : RDD[Profile], attributes : List[String]) : RDD[(Long, String)] = {
  profiles.map{profile =>
    val attributesToUse = profile.attributes.filter(kv => attributes.contains(kv.key))

    val tokens = attributesToUse.flatMap{kv =>
      kv.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING)
    }
    (profile.id, tokens.mkString(" ").toLowerCase)
  }.filter(_._2.trim.size > 0)
}

def test(profiles1 : RDD[Profile], profiles2 : RDD[Profile], cluster : KeysCluster) : (RDD[(Long, String)], RDD[(Long, String)]) = {
  val attributesDataset1 = cluster.keys.filter(_.startsWith(LSHMio.Settings.FIRST_DATASET_PREFIX)).map(_.replace(LSHMio.Settings.FIRST_DATASET_PREFIX, ""))
  val attributesDataset2 = cluster.keys.filter(_.startsWith(LSHMio.Settings.SECOND_DATASET_PREFIX)).map(_.replace(LSHMio.Settings.SECOND_DATASET_PREFIX, ""))
  (getTokens(profiles1, attributesDataset1), getTokens(profiles2, attributesDataset2))
}

val defaultLogPath = "/marconi/home/userexternal/lgaglia1/logs/"

val taskID = "testPPCitations"
val logPath = defaultLogPath + taskID + ".txt"

val dataset = "citations"
//val realProfileID = "realProfileID"
val realProfileID = "id"
val basePath = "/marconi_work/EOI_simonini_0/ER/"

val pathDataset1 = basePath+dataset+"/dataset1.csv"
val pathDataset2 = basePath+dataset+"/dataset2.csv"
val pathGt = basePath+dataset+"/groundtruth.csv"

val dataset1Type = WRAPPER_TYPES.csv
val dataset2Type = WRAPPER_TYPES.csv
val groundtruthType = WRAPPER_TYPES.csv

val hashNum = 256
val clusterThreshold = 0.2
val clusterMaxFactor = 1.0
val clusterSeparateAttributes = true
val smin = 0.4

val sc = SparkContext.getOrCreate()

val log: Logger = LogManager.getRootLogger
log.setLevel(Level.INFO)
val layout = new SimpleLayout();
val appender = new FileAppender(layout, logPath, false);
log.addAppender(appender);


val startTime = Calendar.getInstance()
val dataset1 = {
  if (dataset1Type == WRAPPER_TYPES.serialized) {
    SerializedObjectLoader.loadProfiles(pathDataset1)
  }
  else if (dataset1Type == WRAPPER_TYPES.json) {
    JSONWrapper.loadProfiles(pathDataset1, 0, realProfileID)
  }
  else {
    CSVWrapper.loadProfiles(filePath = pathDataset1, header = true, realIDField = realProfileID)
  }
}
dataset1.cache()
val profilesDataset1 = dataset1.count()

val separatorID = dataset1.map(_.id).max()

val dataset2: RDD[Profile] = {
  if (dataset2Type == WRAPPER_TYPES.serialized) {
    SerializedObjectLoader.loadProfiles(pathDataset2, separatorID + 1)
  }
  else if (dataset2Type == WRAPPER_TYPES.json) {
    JSONWrapper.loadProfiles(pathDataset2, separatorID + 1, realProfileID)
  }
  else {
    CSVWrapper.loadProfiles(filePath = pathDataset2, startIDFrom = separatorID + 1, header = true, realIDField = realProfileID)
  }
}

dataset2.cache()
val profilesDataset2 = dataset2.count()
val profiles: RDD[Profile] = dataset1.union(dataset2)
profiles.cache()
val numProfiles = profiles.count()

log.info("SPARKER - First dataset max ID " + separatorID)
val profilesTime = Calendar.getInstance()
log.info("SPARKER - Number of profiles in the 1st dataset " + profilesDataset1)
log.info("SPARKER - Number of profiles in the 2nd dataset " + profilesDataset2)
log.info("SPARKER - Total number of profiles " + numProfiles)
log.info("SPARKER - Time to load profiles " + msToMin(profilesTime.getTimeInMillis - startTime.getTimeInMillis) + " min")

var startingBlockingTime = Calendar.getInstance()
val clusters = LSHMio.clusterSimilarAttributes2(profiles = profiles, numHashes = hashNum, targetThreshold = clusterThreshold, maxFactor = clusterMaxFactor, separateAttributes = clusterSeparateAttributes, separatorID = separatorID)
val endClusterTime = Calendar.getInstance()
log.info("SPARKER - Generated clusters "+clusters.size)
clusters.foreach{c => log.info("SPARKER - "+c)}
log.info("SPARKER - Time to generate clusters " + msToMin(endClusterTime.getTimeInMillis - startingBlockingTime.getTimeInMillis) + " min")
log.info("SPARKER - Number of clusters " + clusters.size)
profiles.unpersist()


val clustersWithoutGenericCluster = clusters.filter(!_.keys.contains(LSHMio.Settings.DEFAULT_CLUSTER_NAME))
val bb = clustersWithoutGenericCluster

val eMax = bb.map(_.entropy).max
log.info("SPARKER - Max cluster entropy "+eMax)

val aa = bb//bb.filter(_.entropy > (eMax/2.0))
log.info("SPARKER - Retained clusters "+aa.size)

val result = aa.map{cluster =>
  val newDatasets = {
    test(dataset1, dataset2, cluster)
  }

  log.info("SPARKER - Performing PPJOIN for cluster "+cluster)

  val t = Math.min(1.0, smin*eMax/cluster.entropy)

  log.info("SPARKER - PPJOIN threshold "+t)

  val d = PPJoinFn(newDatasets._1, newDatasets._2, separatorID, t)
  val num = d.count()
  log.info("SPARKER - Done, retained candidates "+num)
  d
}

log.info("SPARKER - Retained candidates per cluster "+result.map(x => x.count()))

val candidates = result.reduce((rdd1, rdd2) => rdd1.union(rdd2)).distinct()
candidates.cache()
log.info("SPARKER - Total candidates "+candidates.count())
dataset1.unpersist()
dataset2.unpersist()

val ssGtTime = Calendar.getInstance()
var newGT: RDD[(Long, Long)] = null
var newGTSize :Long = 0

if(pathGt != null){
  log.info("SPARKER - Start to loading the groundtruth")
  val groundtruth = {
    if (groundtruthType == WRAPPER_TYPES.serialized) {
      SerializedObjectLoader.loadGroundtruth(pathGt)
    }
    else if (groundtruthType == WRAPPER_TYPES.json) {
      JSONWrapper.loadGroundtruth(pathGt, "id1", "id2")
    }
    else {
      CSVWrapper.loadGroundtruth(filePath = pathGt, header = true)
    }
  }
  val gtNum = groundtruth.count()

  if (dataset2 != null) {
    val realIdId1 = sc.broadcast(dataset1.map { p =>
      (p.originalID, p.id)
    }.collectAsMap())
    val realIdId2 = sc.broadcast(dataset2.map { p =>
      (p.originalID, p.id)
    }.collectAsMap())
    log.info("SPARKER - Start to generate the new groundtruth")
    newGT = groundtruth.map { g =>
      val first = realIdId1.value.get(g.firstEntityID)
      val second = realIdId2.value.get(g.secondEntityID)
      if (first.isDefined && second.isDefined) {
        (first.get, second.get)
      }
      else {
        (-1L, -1L)
      }
    }.filter(_._1 >= 0)
    realIdId1.unpersist()
    realIdId2.unpersist()
  }
  else {
    val realIdId1 = sc.broadcast(dataset1.map { p =>
      (p.originalID, p.id)
    }.collectAsMap())
    log.info("SPARKER - Start to generate the new groundtruth")
    newGT = groundtruth.map { g =>
      val first = realIdId1.value.get(g.firstEntityID)
      val second = realIdId1.value.get(g.secondEntityID)
      if (first.isDefined && second.isDefined) {
        (first.get, second.get)
      }
      else {
        (-1L, -1L)
      }
    }.filter(_._1 >= 0)
    realIdId1.unpersist()
  }

  newGTSize = newGT.count()
  log.info("SPARKER - Generation completed")
  log.info("SPARKER - Number of elements in the new groundtruth " + newGTSize)
  groundtruth.unpersist()
  val gtTime = Calendar.getInstance()
  log.info("SPARKER - Time to generate the new groundtruth " + msToMin(gtTime.getTimeInMillis - ssGtTime.getTimeInMillis) + " min")
}

val pcPq = ER.estimantePCPQ(newGT, candidates)

log.info("SPARKER - PC "+pcPq._1)
log.info("SPARKER - PQ "+pcPq._2)
val endTime = Calendar.getInstance()
log.info("SPARKER - Total execution time "+msToMin(endTime.getTimeInMillis-startTime.getTimeInMillis))
