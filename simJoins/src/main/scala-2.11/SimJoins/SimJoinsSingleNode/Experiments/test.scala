package SimJoins.SimJoinsSingleNode.Experiments

import java.io.{File, PrintWriter}
import java.util.Calendar

import SimJoins.SimJoinsSingleNode.Commons.CommonFunctions
import SimJoins.SimJoinsSingleNode.SimJoins.GraphJoinAND.sortTypes
import SimJoins.SimJoinsSingleNode.SimJoins.{GraphJoinAND, GraphJoinOrAnd, PpEdJoin}

object test {

  def main(args: Array[String]): Unit = {
    val andCondition1 = Map(("title", (0.8, "JS")), ("authors", (0.5, "JS")))
    val andCondition2 = Map(("title", (3.0, "ED")), ("venue", (0.5, "JS")))

    val conditionsOR: List[Map[String, (Double, String)]] = List(
      andCondition1,
      andCondition2
    )

    val log1 = new PrintWriter(new File("C:/Users/gagli/Desktop/graphjoin.txt"))
    val log2 = new PrintWriter(new File("C:/Users/gagli/Desktop/ppedjoin.txt"))

    val profiles = CommonFunctions.loadData("C:/Users/gagli/Desktop/ACM.csv")

    val res = for (i <- 0 until 10) yield {
      val t1 = Calendar.getInstance().getTimeInMillis
      val r1 = GraphJoinOrAnd.getMatchesMultiSort(profiles, conditionsOR, log1, sortTypes.thresholdAsc)
      val t2 = Calendar.getInstance().getTimeInMillis


      val r2 = PpEdJoin.getMatchesAndOr(profiles, conditionsOR, log2)
      val t3 = Calendar.getInstance().getTimeInMillis

      (i, t2 - t1, t3 - t2)
    }

    val tc = res.toList.filter(_._1 > 3).map(_._2)
    val to = res.toList.filter(_._1 > 3).map(_._3)

    println("Tempo medio GraphJoin "+tc.sum/tc.size)
    println("Tempo medio PPJoin "+to.sum/to.size)

    log1.close()
    log2.close()


    /*
    val r1 = GraphJoinOrAnd.getMatchesMultiSort(profiles, conditionsOR, log1, sortTypes.thresholdAsc)
    val r2 = PpEdJoin.getMatchesAndOr(profiles, conditionsOR, log2)

    println("Numero risultati metodo vecchio " + r1.length)
    println("Numero risultati metodo nuovo " + r2.length)

    val commons = r1.intersect(r2)
    val d1 = r1.diff(commons)
    val d2 = r2.diff(commons)

    println("Metodo vecchio - risultati comuni " + d1.length)
    println("Metodo nuovo - risultati comuni " + d2.length)

    d1.foreach(println)
    d2.foreach(println)

    val s1 = d1.union(d2).flatMap(x => List(x._1, x._2)).toSet
    profiles.filter(pid => s1.contains(pid.id)).foreach(println)
    */


  }
}
