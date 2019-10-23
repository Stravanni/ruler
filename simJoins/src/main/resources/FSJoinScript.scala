import java.util.Calendar

import utils.Tokenizer
import DataPreparation.PrefixDataBuilder
import JoinEngine.TTJoin
import VerifyFunctions.JaccardOrdered
import org.apache.log4j._
import org.apache.spark.SparkContext


val dataPath = "/marconi_work/EOI_simonini_0/SimJoin/datasets/mail/emails.txt"
val logPath = "/marconi/home/userexternal/lgaglia1/logs/ttjoin.txt"
val threshold = 0.9

//FSJoinFinal.FSJoin(dataPath, threshold, logPath)

val sc = SparkContext.getOrCreate()

val log: Logger = LogManager.getRootLogger
log.setLevel(Level.INFO)
val layout = new SimpleLayout()
val appender = new FileAppender(layout, logPath, false)
log.addAppender(appender)

val startTime = Calendar.getInstance()

log.info("SPARKER - SOGLIA "+threshold)

val t1 = Calendar.getInstance()
val documents = FSJoin.loadData(dataPath)
documents.cache()
documents.count()
val t2 = Calendar.getInstance()
log.info("SPARKER - DOCUMENTI CARICATI " + FSJoin.msToMin(t2.getTimeInMillis - t1.getTimeInMillis))

val tokenizedData = PrefixDataBuilder.prepareAllTokens(documents, Tokenizer.Word)
tokenizedData.cache()
tokenizedData.count()
documents.unpersist()
val t3 = Calendar.getInstance()
log.info("SPARKER - DOCUMENTI TOKENIZZATI " + FSJoin.msToMin(t3.getTimeInMillis - t2.getTimeInMillis))


val verified = TTJoin.joinAllTokensVerifyBalancing(tokenizedData, threshold)(JaccardOrdered.verify)
val verifiedNum = verified.count()
val t4 = Calendar.getInstance()
log.info("SPARKER - NUMERO ELEMENTI MANTENUTI " + verifiedNum)

val endTime = Calendar.getInstance()

log.info("SPARKER - Total execution time " + FSJoin.msToMin(endTime.getTimeInMillis - startTime.getTimeInMillis))
