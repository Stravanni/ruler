package SimJoins.SimJoinsSingleNode.Experiments

import java.io.{File, PrintWriter}
import java.util.Calendar

import SimJoins.SimJoinsSingleNode.Commons.CommonFunctions
import SimJoins.SimJoinsSingleNode.SimJoins.GraphJoinAND.sortTypes
import SimJoins.SimJoinsSingleNode.SimJoins.{GraphJoinAND, GraphJoinOrAnd}

object test {


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

  def main(args: Array[String]): Unit = {
    val andCondition1 = Map(("title", (0.8, "JS")), ("authors", (0.5, "JS")))
    val andCondition2 = Map(("title", (3.0, "ED")), ("venue", (0.5, "JS")))

    val conditionsOR: List[Map[String, (Double, String)]] = List(
      andCondition1,
      andCondition2
    )

    val log1 = new PrintWriter(new File("C:/Users/gagli/Desktop/c1.txt"))
    val log2 = new PrintWriter(new File("C:/Users/gagli/Desktop/c2.txt"))
    val log3 = new PrintWriter(new File("C:/Users/gagli/Desktop/c1orc2.txt"))

    val profiles = CommonFunctions.loadData("C:/Users/gagli/Desktop/ACM.csv")


    val res = for (i <- 0 until 10) yield {
      val t1 = Calendar.getInstance().getTimeInMillis
      val r1_1 = GraphJoinAND.getMatchesMultiSort(profiles, andCondition1, log1, sortTypes.thresholdAsc)
      val r1_2 = GraphJoinAND.getMatchesMultiSort(profiles, andCondition2, log2, sortTypes.thresholdAsc)
      val r1 = r1_1.union(r1_2).distinct
      val t2 = Calendar.getInstance().getTimeInMillis


      val r2 = GraphJoinOrAnd.getMatchesMultiSort(profiles, conditionsOR, log3, sortTypes.thresholdAsc)
      val t3 = Calendar.getInstance().getTimeInMillis

      ((t2 - t1), (t3 - t2))

      //println("Tempo combinato " + (t2 - t1))
      //println("Tempo ottimizzato " + (t3 - t2))
    }

    val tc = res.toList.tail.map(_._1)
    val to = res.toList.tail.map(_._2)

    println("Tempo medio or fatto a mano "+tc.sum/tc.size)
    println("Tempo medio or ottimizzato "+to.sum/to.size)


    /*println("Numero risultati metodo vecchio "+r1.length)
    println("Numero risultati metodo nuovo "+r2.length)

    val commons = r1.intersect(r2)
    val d1 = r1.diff(commons)
    val d2 = r2.diff(commons)

    println("Metodo vecchio - risultati comuni "+d1.length)
    println("Metodo nuovo - risultati comuni "+d2.length)

    d1.foreach(println)
    d2.foreach(println)

    val s1 = d1.union(d2).flatMap(x => List(x._1, x._2)).toSet
    profiles.filter(pid => s1.contains(pid.id)).foreach(println)*/

  }
}
