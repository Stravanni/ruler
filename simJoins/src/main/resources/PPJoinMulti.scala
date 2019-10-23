import java.util.Calendar

import BlockBuildingMethods.LSHMio
import DataPreparation.PrefixDataBuilder
import DataStructures.Profile
import JoinEngine.{ERJoin, PPJoin}
import Utilities.ER
import Wrappers.{CSVWrapper, JSONWrapper, SerializedObjectLoader}
import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import utils.Tokenizer

object WRAPPER_TYPES {
  val serialized = "serialized"
  val csv = "csv"
  val json = "json"
}

def msToMin(ms: Long): Double = (ms / 1000.0) / 60.0


val defaultLogPath = "/marconi/home/userexternal/lgaglia1/logs/"

val taskID = "testPPJOINCitationsMulti"
val logPath = defaultLogPath + taskID + ".txt"

val dataset = "citations"
//val realProfileID = "realProfileID"
val realProfileID = "id"
val basePath = "/marconi_work/EOI_simonini_0/ER/"

val pathDataset1 = basePath + dataset + "/dataset1.csv"
val pathDataset2 = basePath + dataset + "/dataset2.csv"
val pathGt = basePath + dataset + "/groundtruth.csv"

val dataset1Type = WRAPPER_TYPES.csv
val dataset2Type = WRAPPER_TYPES.csv
val groundtruthType = WRAPPER_TYPES.csv

val hashNum = 256
val clusterMaxFactor = 1.0
val clusterSeparateAttributes = true
val thresholds = List(0.8, 0.6, 0.4, 0.2)

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
dataset1.unpersist()
dataset2.unpersist()

log.info("SPARKER - First dataset max ID " + separatorID)
val profilesTime = Calendar.getInstance()
log.info("SPARKER - Number of profiles in the 1st dataset " + profilesDataset1)
log.info("SPARKER - Number of profiles in the 2nd dataset " + profilesDataset2)
log.info("SPARKER - Total number of profiles " + numProfiles)
log.info("SPARKER - Time to load profiles " + msToMin(profilesTime.getTimeInMillis - startTime.getTimeInMillis) + " min")

val ssGtTime = Calendar.getInstance()
var newGT: RDD[(Long, Long)] = null
var newGTSize: Long = 0

if (pathGt != null) {
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


log.info("SPARKER - Tokenization")
val data = profiles.map(x => (x.id, x.attributes.map(_.value).mkString(" ")))
data.cache()
data.count()

val sTokenTime = Calendar.getInstance()
val tokenizedData = PrefixDataBuilder.prepareAllTokens(data, Tokenizer.Word)
tokenizedData.cache()
tokenizedData.count()
val eTokenTime = Calendar.getInstance()
log.info("SPARKER - Tokenization time " + msToMin(eTokenTime.getTimeInMillis - sTokenTime.getTimeInMillis))
data.unpersist()


thresholds.foreach { threshold =>
  log.info("SPARKER - Threshold " + threshold)
  val startLshTime = Calendar.getInstance()
  val lshBlocks = LSHMio.createBlocks(profiles, numHashes = hashNum, targetThreshold = threshold, separatorID = separatorID)
  lshBlocks.cache()
  val numBlocks = lshBlocks.count()
  log.info("SPARKER - Numero blocchi " + numBlocks)
  val endLshTime = Calendar.getInstance()
  log.info("SPARKER - LSH time " + msToMin(endLshTime.getTimeInMillis - startLshTime.getTimeInMillis))

  val lshCandidates = lshBlocks.flatMap(_.getAllPairs()).distinct()
  lshCandidates.cache()
  val numLshCandidates = lshCandidates.count()
  lshBlocks.unpersist()
  log.info("SPARKER - Num LSH Candidates " + numLshCandidates)
  val pcPqLSH = ER.estimantePCPQ(newGT, lshCandidates)
  log.info("SPARKER - LSH PC " + pcPqLSH._1)
  log.info("SPARKER - LSH PQ " + pcPqLSH._2)
  lshCandidates.unpersist()

  val sPPTime = Calendar.getInstance()
  val ppJoinCandidates = PPJoin.rddJoinVerifyADBalanced(tokenizedData, separatorID, threshold)
  ppJoinCandidates.cache()
  val numPPJoinCandidates = ppJoinCandidates.count()
  val ePPTime = Calendar.getInstance()
  log.info("SPARKER - Num ppjoin candidates " + numPPJoinCandidates)
  log.info("SPARKER - PPJOIN time " + msToMin(ePPTime.getTimeInMillis - sPPTime.getTimeInMillis))
  val pcPqPP = ER.estimantePCPQ(newGT, ppJoinCandidates.map(x => (x._1._1, x._2._1)))
  log.info("SPARKER - PPJOIN PC " + pcPqPP._1)
  log.info("SPARKER - PPJOIN PQ " + pcPqPP._2)
  ppJoinCandidates.unpersist()


  val sERTime = Calendar.getInstance()
  val ERJoinCandidates = ERJoin.rddJoinVerifyBalanced(tokenizedData, separatorID, threshold)
  ERJoinCandidates.cache()
  val numERJoinCandidates = ERJoinCandidates.count()
  val eERTime = Calendar.getInstance()
  log.info("SPARKER - Num ppjoin candidates " + numERJoinCandidates)
  log.info("SPARKER - ERJOIN time " + msToMin(eERTime.getTimeInMillis - sERTime.getTimeInMillis))
  val pcPqER = ER.estimantePCPQ(newGT, ERJoinCandidates)
  log.info("SPARKER - ERJOIN PC " + pcPqER._1)
  log.info("SPARKER - ERJOIN PQ " + pcPqER._2)
  ppJoinCandidates.unpersist()
}

  val endTime = Calendar.getInstance()
  log.info("SPARKER - Total execution time " + msToMin(endTime.getTimeInMillis - startTime.getTimeInMillis))