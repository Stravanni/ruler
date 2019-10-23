import java.util.Calendar

import DataPreparation.PrefixDataBuilder
import DataStructures.Profile
import JoinEngine.{PPJoin}
import VerifyFunctions.JaccardObj
import Wrappers.{CSVWrapper, JSONWrapper, SerializedObjectLoader}
import org.apache.log4j._
import org.apache.spark.ml.feature.StopWordsRemover
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

val taskID = "logTestNewImp"
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
val thresholds = List(0.8, 0.4)

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

//val stopWords = sc.broadcast(StopWordsRemover.loadDefaultStopWords("english").map(_.toLowerCase))
//val data = profiles.map(x => (x.id, x.attributes.flatMap(_.value.trim.toLowerCase).filter(x => !stopWords.value.contains(x)).mkString(" ").toLowerCase))
val data = profiles.map(x => (x.id, x.attributes.flatMap(_.value.trim.toLowerCase).mkString(" ").toLowerCase))
data.cache()
data.count()
//stopWords.unpersist()

val threshold = 0.8

log.info("SPARKER - GIANGIGI GIGIAVA")

val t3 = Calendar.getInstance()
val c = PPJOINMIOClean3.join(data, threshold, separatorID)
val n = c.count()
val t4 = Calendar.getInstance()
log.info("SPARKER - Tempo PPJOIN Mio " + msToMin(t4.getTimeInMillis-t3.getTimeInMillis))
log.info("SPARKER - Numero risultati PPJOIN Mio " + n)

val t1 = Calendar.getInstance()
val tokenizedData = PrefixDataBuilder.prepareAllTokens(data, Tokenizer.Word)
val candidates = PPJoin.rddJoinVerifyADBalanced(tokenizedData, separatorID, threshold)
val verified = candidates.filter(c => JaccardObj.verify(c._1._2, c._2._2, threshold))
val verifiedNum = verified.count()
val t2 = Calendar.getInstance()


log.info("SPARKER - Tempo PPJOIN Song " + msToMin(t2.getTimeInMillis-t1.getTimeInMillis))
log.info("SPARKER - Numero risultati PPJOIN Song " + verifiedNum)
log.info("SPARKER - Tempo PPJOIN Mio " + msToMin(t4.getTimeInMillis-t3.getTimeInMillis))
log.info("SPARKER - Numero risultati PPJOIN Mio " + n)

val endTime = Calendar.getInstance()
log.info("SPARKER - Total execution time " + msToMin(endTime.getTimeInMillis - startTime.getTimeInMillis))