package SimJoins.Spark.Experiments

import java.io.{File, PrintWriter}
import java.util.Calendar

import SimJoins.Spark.Commons.CommonFunctions
import SimJoins.Spark.SimJoins.{EDJoin, PPJoin}
import org.apache.spark.{SparkConf, SparkContext}

object Main {


  def testPPJoin(): Unit = {
    val t1 = Calendar.getInstance().getTimeInMillis
    val profiles = CommonFunctions.loadProfiles("C:/Users/gagli/Desktop/UNI/ACM.csv", header = true, realIDField = "id")

    val conditions: Map[String, (Double, String)] = Map(("title", (0.7, "PPJOIN")), ("authors", (0.5, "PPJOIN")))

    //val conditions: Map[String, (Double, String)] = Map(("title", (0.4, "PPJOIN")))

    val c = conditions.map(x => (x._1, x._2._1))

    val pairs = PPJoin.getMatchesMulti(profiles, c)

    val numPairs = pairs.count()
    val t2 = Calendar.getInstance().getTimeInMillis

    val profiles2 = SimJoins.SimJoinsSingleNode.Commons.CommonFunctions.loadData("C:/Users/gagli/Desktop/UNI/ACM.csv", realProfileId = "id")
    val pairs2 = SimJoins.SimJoinsSingleNode.SimJoins.PPJoin.getMatchesMulti(profiles2, c, new PrintWriter(new File("C:/Users/Gagli/Desktop/aacid.txt")))
    val numPairs2 = pairs2.length
    val t3 = Calendar.getInstance().getTimeInMillis

    println(numPairs)
    println(numPairs2)


    println("Spark " + SimJoins.SimJoinsSingleNode.Commons.CommonFunctions.msToMin(t2 - t1))
    println("Seriale " + SimJoins.SimJoinsSingleNode.Commons.CommonFunctions.msToMin(t3 - t2))
  }

  def testEDJoin(): Unit ={
    val t1 = Calendar.getInstance().getTimeInMillis
    val profiles = CommonFunctions.loadProfiles("C:/Users/gagli/Desktop/UNI/ACM.csv", header = true, realIDField = "id")

    val conditions: Map[String, (Double, String)] = Map(("title", (3, "EDJOIN")))

    val c = conditions.map(x => (x._1, x._2._1))

    val pairs = EDJoin.getMatchesMulti(profiles, c, 2)

    val numPairs = pairs.count()
    val t2 = Calendar.getInstance().getTimeInMillis

    val profiles2 = SimJoins.SimJoinsSingleNode.Commons.CommonFunctions.loadData("C:/Users/gagli/Desktop/UNI/ACM.csv", realProfileId = "id")
    val pairs2 = SimJoins.SimJoinsSingleNode.SimJoins.EDJoin.getMatchesMulti(profiles2, c, new PrintWriter(new File("C:/Users/Gagli/Desktop/aacid.txt")), 2)
    val numPairs2 = pairs2.length
    val t3 = Calendar.getInstance().getTimeInMillis

    println(numPairs)
    println(numPairs2)


    println("Spark "+SimJoins.SimJoinsSingleNode.Commons.CommonFunctions.msToMin(t2-t1))
    println("Seriale "+SimJoins.SimJoinsSingleNode.Commons.CommonFunctions.msToMin(t3-t2))


    val diff = pairs2.diff(pairs.collect())
    val aa = diff.flatMap(x => List(x._1, x._2)).toSet
    profiles2.filter(p => aa.contains(p.id)).foreach(println)

    println()
    println()
    diff.foreach(println)
  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Main")
      .setMaster("local[*]")
      .set("spark.local.dir", "/data2/sparkTmp/")
    val sc = new SparkContext(conf)

    testEDJoin()


  }

}
