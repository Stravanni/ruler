package SimJoins.SimJoinsSingleNode.Commons

import SimJoins.DataStructure.{KeyValue, Profile}
import org.apache.commons.csv.CSVFormat
import java.io.FileReader

import scala.collection.JavaConversions._
import scala.io.Source

/**
  * Created by Luca on 27/03/2018.
  */
object CommonFunctions {
  def msToMin(ms: Long): Double = (ms / 1000.0) / 60.0

  def extractField(profiles: List[Profile], fieldName: String): List[(Long, String)] = {
    profiles.map { profile =>
      (profile.id, profile.attributes.filter(_.key == fieldName).map(_.value).mkString(" ").toLowerCase)
    }.filter(!_._2.trim.isEmpty)
  }

  def loadUnstructuredData(path: String): List[(Long, String)] = {
    val docs = for (line <- Source.fromFile(path).getLines()) yield {
      line
    }
    docs.zipWithIndex.map(x => (x._2.toLong, x._1)).toList
  }

  def loadData(path: String, separator: Char = ',', realProfileId: String = "", startIdFrom: Long = 0): List[Profile] = {

    val in = new FileReader(path)
    val records = CSVFormat.RFC4180.withFirstRecordAsHeader.parse(in)
    //val records = CSVFormat.newFormat(separator).withFirstRecordAsHeader().parse(in)


    val profiles = records.zipWithIndex.map { case (record, index) =>
      val realId = {
        if (realProfileId.isEmpty) {
          ""
        }
        else {
          record.get(realProfileId)
        }
      }
      val profile = Profile(id = index + startIdFrom, originalID = realId)
      records.getHeaderMap.foreach { case (fieldName, fieldNum) =>
        if (fieldName != realProfileId) {
          val value = record.get(fieldName).trim
          if (!value.isEmpty) {
            profile.addAttribute(KeyValue(fieldName, value))
          }
        }
      }
      profile
    }

    profiles.toList
  }
}
