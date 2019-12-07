package RulER.SimJoins

import java.util.Calendar

import RulER.Commons.DataVerificator
import RulER.DataStructure.CommonClasses.{tokenized, tokensED, tokensJs}
import RulER.DataStructure.ThresholdTypes
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD

object JoinChain {

  def getMatches(tokenizedProfiles: RDD[(Long, Map[String, Map[String, tokenized]])],
                 conditionsOR: List[Map[String, (Double, String)]],
                 qgramLength: Int = 2): RDD[(Long, Long)] = {

    val log = LogManager.getRootLogger

    val s1 = Calendar.getInstance().getTimeInMillis
    val orRes = conditionsOR.map { andBlock =>
      val candidatesAnd = andBlock.map { case (attribute, (threshold, thresholdType)) =>
        if (thresholdType == ThresholdTypes.JS) {
          val p = tokenizedProfiles.map { case (profileID, data) => (profileID, data(thresholdType)(attribute).asInstanceOf[tokensJs].tokens) }
          PPJoin.getCandidates2(p, threshold)
        }
        else {
          val p = tokenizedProfiles.map { case (profileID, data) => (profileID, data(thresholdType)(attribute).asInstanceOf[tokensED].qgrams) }
          EDJoin.getCandidates2(p, qgramLength, threshold.toInt)
        }
      }
      candidatesAnd.reduce((x, y) => x.intersection(y))
    }
    val candidates = orRes.reduce((x, y) => x.union(y)).distinct()
    candidates.cache()
    val nc = candidates.count()
    val s2 = Calendar.getInstance().getTimeInMillis
    log.info("[GraphJoin] RuleChain join time " + (s2 - s1) / 1000.0 / 60.0)
    log.info("[GraphJoin] Num candidates " + nc)

    val docTokens = tokenizedProfiles.context.broadcast(tokenizedProfiles.collectAsMap())

    val verified = DataVerificator.verify(candidates, docTokens, conditionsOR)
    verified.cache()
    val nm = verified.count()
    val s3 = Calendar.getInstance().getTimeInMillis
    log.info("[GraphJoin] RuleChain verify time " + (s3 - s2) / 1000.0 / 60.0)
    log.info("[GraphJoin] Num matches " + nm)
    docTokens.unpersist()

    verified
  }

  def getMatchesVernica(tokenizedProfiles: RDD[(Long, Map[String, Map[String, tokenized]])],
                        conditionsOR: List[Map[String, (Double, String)]],
                        qgramLength: Int = 2): RDD[(Long, Long)] = {

    val log = LogManager.getRootLogger

    val s1 = Calendar.getInstance().getTimeInMillis
    val orRes = conditionsOR.map { andBlock =>
      val candidatesAnd = andBlock.map { case (attribute, (threshold, thresholdType)) =>
        if (thresholdType == ThresholdTypes.JS) {
          val p = tokenizedProfiles.map { case (profileID, data) => (profileID, data(thresholdType)(attribute).asInstanceOf[tokensJs].tokens) }
          PPJoin2.getCandidates(p, threshold)
        }
        else {
          val p = tokenizedProfiles.map { case (profileID, data) => (profileID, data(thresholdType)(attribute).asInstanceOf[tokensED].qgrams) }
          EDJoin.getCandidates2(p, qgramLength, threshold.toInt)
        }
      }
      candidatesAnd.reduce((x, y) => x.intersection(y))
    }
    val candidates = orRes.reduce((x, y) => x.union(y)).distinct()
    candidates.cache()
    val nc = candidates.count()
    val s2 = Calendar.getInstance().getTimeInMillis
    log.info("[GraphJoin] RuleChain join time " + (s2 - s1) / 1000.0 / 60.0)
    log.info("[GraphJoin] Num candidates " + nc)

    val docTokens = tokenizedProfiles.context.broadcast(tokenizedProfiles.collectAsMap())

    val verified = DataVerificator.verify(candidates, docTokens, conditionsOR)
    verified.cache()
    val nm = verified.count()
    val s3 = Calendar.getInstance().getTimeInMillis
    log.info("[GraphJoin] RuleChain verify time " + (s3 - s2) / 1000.0 / 60.0)
    log.info("[GraphJoin] Num matches " + nm)
    docTokens.unpersist()

    verified
  }
}
