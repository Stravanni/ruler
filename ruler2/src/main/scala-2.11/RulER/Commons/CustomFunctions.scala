package RulER.Commons

import RulER.DataStructure.{KeyValue, Profile, RulesOr}
import RulER.SimJoins.RulERJoin
import org.apache.spark.sql.{DataFrame, SparkSession}

class CustomFunctions(df: DataFrame) {
  def numRows: Long = df.count()

  def joinWithRules(df2: DataFrame, rules: RulesOr, id1: String = "", id2: String = ""): DataFrame = {
    if (df == df2) {
      val profiles = CommonFunctions.dfProfilesToRDD(df, realIDField = id1)
      val results = RulERJoin.getMatches(profiles, rules.getRules)
      val p = profiles.context.broadcast(profiles.map(p => (p.id, p.originalID)).collectAsMap())
      val originalIDs = results.map { case (id1, id2) =>
        (p.value(id1), p.value(id2))
      }

      val sparkSession = SparkSession.builder().getOrCreate()
      sparkSession.createDataFrame(originalIDs).toDF("id1", "id2")
    }
    else {
      //TODO
      val sparkSession = SparkSession.builder().getOrCreate()
      sparkSession.createDataFrame(List((1, 2), (3, 4))).toDF("id1", "id2")
    }
  }

  def joinWithRules2(df2: DataFrame, rules: RulesOr, id1: String = "", id2: String = ""): DataFrame = {
    if (df == df2) {
      val profiles = CommonFunctions.dfProfilesToRDD(df, realIDField = id1)
      val results = RulERJoin.getMatches(profiles, rules.getRules)
      val p = profiles.context.broadcast(profiles.map(p => (p.id, p.originalID)).collectAsMap())
      val originalIDs = results.map { case (id1, id2) =>
        (p.value(id1), p.value(id2))
      }

      val sparkSession = SparkSession.builder().getOrCreate()
      val res = sparkSession.createDataFrame(originalIDs).toDF("id1", "id2")

      val r1 = df.join(res, df(id1) === res("id1"))
      df.join(r1, r1("id2") === df(id1))
    }
    else {
      //TODO
      val sparkSession = SparkSession.builder().getOrCreate()
      sparkSession.createDataFrame(List((1, 2), (3, 4))).toDF("id1", "id2")
    }
  }

  def joinWithRules3(df2: DataFrame, rules: List[List[(String, String, String, Double)]], id1: String = "", id2: String = ""): DataFrame = {
    val profiles1 = CommonFunctions.dfProfilesToRDD(df, realIDField = id1)
    val max = profiles1.map(_.id).max()
    val profiles2 = CommonFunctions.dfProfilesToRDD(df2, realIDField = id2, startIDFrom = max)

    val cindex = rules.flatten.zipWithIndex.toMap
    val attr1 = cindex.map(x => (x._1._1, "c" + x._2))
    val attr2 = cindex.map(x => (x._1._2, "c" + x._2))

    val cformat = rules.map { cand =>
      cand.map { condition =>
        ("c"+cindex(condition), (condition._4, condition._3))
      }.toMap
    }

    val p1 = profiles1.map { p =>
      Profile(p.id, p.attributes.withFilter(x => attr1.keySet.contains(x.key)).map(x => KeyValue(attr1(x.key), x.value)), originalID = p.originalID)
    }

    val p2 = profiles2.map { p =>
      Profile(p.id, p.attributes.withFilter(x => attr2.keySet.contains(x.key)).map(x => KeyValue(attr2(x.key), x.value)), originalID = p.originalID)
    }

    val profiles = p1.union(p2)
    val results = RulERJoin.getMatches(profiles, cformat, separatorID = max)
    val p1b = profiles.context.broadcast(profiles1.map(p => (p.id, p.originalID)).collectAsMap())
    val p2b = profiles.context.broadcast(profiles2.map(p => (p.id, p.originalID)).collectAsMap())

    val originalIDs = results.map { case (id1, id2) =>
      (p1b.value(id1), p2b.value(id2))
    }

    val sparkSession = SparkSession.builder().getOrCreate()
    val res = sparkSession.createDataFrame(originalIDs).toDF("id1", "id2")

    val r1 = df.join(res, df(id1) === res("id1"))
    df2.join(r1, r1("id2") === df2(id2))
  }

  def joinWithRules4(df2: DataFrame, rules: List[List[(String, String, String, Double)]], id1: String = "", id2: String = ""): DataFrame = {

    val profiles1 = CommonFunctions.dfProfilesToRDD(df, realIDField = id1)
    val max = profiles1.map(_.id).max()
    val profiles2 = CommonFunctions.dfProfilesToRDD(df2, realIDField = id2, startIDFrom = max + 1)

    val conditionsOr = rules

    val cindex = conditionsOr.flatten.zipWithIndex.toMap
    val attr1 = cindex.map(x => (x._1._1, "c" + x._2))
    val attr2 = cindex.map(x => (x._1._2, "c" + x._2))

    val cformat = conditionsOr.map { cand =>
      cand.map { condition =>
        ("c"+cindex(condition), (condition._4, condition._3))
      }.toMap
    }
    val p1 = profiles1.map { p =>
      Profile(p.id, p.attributes.withFilter(x => attr1.keySet.contains(x.key)).map(x => KeyValue(attr1(x.key), x.value)), originalID = p.originalID)
    }

    val p2 = profiles2.map { p =>
      Profile(p.id, p.attributes.withFilter(x => attr2.keySet.contains(x.key)).map(x => KeyValue(attr2(x.key), x.value)), originalID = p.originalID)
    }

    val pfinal = p1.union(p2)


    val matches2 = RulERJoin.getMatches(pfinal, cformat, separatorID = max)


    val p1b = pfinal.context.broadcast(profiles1.map(p => (p.id, p.originalID)).collectAsMap())
    val p2b = pfinal.context.broadcast(profiles2.map(p => (p.id, p.originalID)).collectAsMap())

    val originalIDs = matches2.map { case (id1, id2) =>
      (p1b.value(id1), p2b.value(id2))
    }

    val sparkSession = SparkSession.builder().getOrCreate()
    val res = sparkSession.createDataFrame(originalIDs).toDF("id1", "id2")

    val r1 = df.join(res, df(id1) === res("id1"))
    df2.join(r1, r1("id2") === df2(id2))
  }
}

object CustomFunctions {
  implicit def addCustomFunctions(df: DataFrame) = new CustomFunctions(df)
}