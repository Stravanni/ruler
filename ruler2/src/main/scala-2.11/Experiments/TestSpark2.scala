package Experiments

import RulER.Commons.{CommonFunctions, DataPreparator}
import java.util.Calendar

import RulER.DataStructure.Profile
import RulER.SimJoins.{EDJoin, JoinChain, PPJoin, RulERJoin}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{FileAppender, Level, LogManager, SimpleLayout}
import org.apache.spark.rdd.RDD

object TestSpark2 {
  def main(args: Array[String]): Unit = {
    val logFilePath = "/data2/test_vari.txt"
    val inputFilePath = "C:/Users/gagli/Desktop/imdb_sm.csv"


    val conf = new SparkConf()
      .setAppName("Main")
      .setMaster("local[*]")
      .set("spark.default.parallelism", "16")
      .set("spark.local.dir", "/data2/tmp")
      .set("spark.driver.maxResultSize", "0")
      .set("spark.executor.heartbeatInterval", "200000")
      .set("spark.network.timeout", "300000")

    val sc = new SparkContext(conf)

    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val layout = new SimpleLayout()
    val appender = new FileAppender(layout, logFilePath, false)
    log.addAppender(appender)

    val profiles = CommonFunctions.loadProfiles(filePath = inputFilePath, header = true, realIDField = "imdbid", separator = ",")
    profiles.cache()
    val numprofiles = profiles.count()
    log.info("[GraphJoin] Number of profiles " + numprofiles)

    //imdbid,title,year,genres,director,writer,cast,runtime,country,language,rating,plot
    val titleJS = ("title", (0.8, "JS"))
    val titleED = ("title", (2.0, "ED"))
    val directorJS = ("director", (0.7, "JS"))
    val castJS = ("cast", (0.7, "JS"))

    val matchingRule = List(
      Map(titleJS, directorJS)
      //Map(castJS, titleED)
    )


    val conditionsPerAttribute = DataPreparator.parseConditions(matchingRule)
    val tokenizedProfiles = DataPreparator.tokenizeProfiles(profiles, conditionsPerAttribute, 5)

    val t1 = Calendar.getInstance().getTimeInMillis
    /*val m1 = RulERJoin.getMatches2(tokenizedProfiles, matchingRule, 5)
    val m2 = JoinChain.getMatches(tokenizedProfiles, matchingRule, 5)*/

    //val m1 = JoinChain.getMatches(tokenizedProfiles, matchingRule, 5)
    val m2 = JoinChain.getMatchesVernica(tokenizedProfiles, matchingRule, 5)

    tokenizedProfiles.filter(x => x._1 == 105 || x._1 == 106).collect().foreach(println)


    val t2 = Calendar.getInstance().getTimeInMillis
    log.info("[GraphJoin] Total time " + (t2 - t1))

    sc.stop()
  }
}
