package Experiments

import RulER.Commons.{CommonFunctions, DataPreparator}
import org.apache.log4j.{FileAppender, Level, LogManager, SimpleLayout}
import org.apache.spark.{SparkConf, SparkContext}

object Test1 {
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

    //id;author;author-aux;author-orcid;booktitle;cdate;cdrom;cite;cite-label;crossref;editor;editor-orcid;ee;ee-type;i;journal;key;mdate;month;note;note-type;number;pages;publisher;publtype;sub;sup;title;title-bibtex;tt;url;volume;year
    val titleJS = ("title", (0.8, "JS"))
    val titleED = ("title", (2.0, "ED"))
    val directorJS = ("director", (0.7, "JS"))
    val castJS = ("cast", (0.7, "JS"))

    val matchingRule = List(
      Map(titleJS, directorJS),
      Map(castJS, titleED)
    )


    val conditionsPerAttribute = DataPreparator.parseConditions(matchingRule)
    val tokenizedProfiles = DataPreparator.tokenizeProfiles(profiles, conditionsPerAttribute, 5)



    sc.stop()
  }

}
