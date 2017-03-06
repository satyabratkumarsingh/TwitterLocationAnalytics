package analysis

import configreader.CassandraConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{lower, upper}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Satya on 05/03/2017.
  */
object TwitterAnalysis extends App {

  val conf = new SparkConf(true).set("spark.cassandra.connection.host", CassandraConfig.hostName)
  val sc = new SparkContext("local[5]", "TwitterAnalysis", conf)
  val sqlContext = new SQLContext(sc)
  sqlContext.setConf("ClusterOne/spark.cassandra.input.split.size_in_mb", "32")
  sqlContext.setConf("default:test/spark.cassandra.input.split.size_in_mb", "128")

  val dataFrameTweets = sqlContext
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> CassandraConfig.table, "keyspace" -> CassandraConfig.keySpace))
    .load()

  val dataFramePositiveWords = sqlContext
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "positivewords", "keyspace" -> "sparkks"))
    .load()

  val dataFrameNegativeWords = sqlContext
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "negativewords", "keyspace" -> "sparkks"))
    .load()

  import sqlContext.implicits._

  val positiveWords = dataFramePositiveWords.select("word").rdd.map(r => r(0).asInstanceOf[String].toLowerCase()).collect()
  val negativeWords = dataFrameNegativeWords.select("word").rdd.map(r => r(0).asInstanceOf[String].toLowerCase()).collect()

  val filteredDataFrameOnBjp = dataFrameTweets.filter(lower($"tweet").contains("modi")
    || lower($"tweet").contains("bjp")
    || lower($"tweet").contains("amit shah")
    || lower($"tweet").contains("amitshah")
    || lower($"tweet").contains("yogi adityanath")
    || lower($"tweet").contains("yogiadityanath")
    || lower($"tweet").contains("rajnath singh")
    || lower($"tweet").contains("rajnathsingh")
    || lower($"tweet").contains("uma bharti")
    || lower($"tweet").contains("umabharti")
    || lower($"tweet").contains("rss/bjp")
    || lower($"tweet").contains("rss-bjp")
    ).select("tweet")

  val filteredDataFrameOnBjpCount = filteredDataFrameOnBjp.count();

  println("Total people tweeting for BJP : " + filteredDataFrameOnBjpCount)

  val bjpPositiveCount = filteredDataFrameOnBjp.filter(row=>sentenceContainsWord(row.getString(0),positiveWords)).count()

  println("Total people talking positively for BJP : " + bjpPositiveCount)

  val bjpNegativeCount = filteredDataFrameOnBjp.filter(row=>sentenceContainsWord(row.getString(0),negativeWords)).count()

  println("Total people talking negatively for BJP : " + bjpNegativeCount)

  val totalBjpCount = bjpPositiveCount+bjpNegativeCount
  val percentageOfBjpTalkingPositively = (bjpPositiveCount.toFloat/totalBjpCount)*100

  println("Percentage of talking positively for BJP is : " + percentageOfBjpTalkingPositively)


  val percentageOfBjpTalkingNegatively = (bjpNegativeCount.toFloat/totalBjpCount)*100

  println("Percentage of talking negatively for BJP is : " + percentageOfBjpTalkingNegatively)


  val bjpMuslimNegativeCount = filteredDataFrameOnBjp.filter(row=>sentenceContainsWord(row.getString(0),negativeWords)).filter(row=>sentenceContainsWord(row.getString(0),Array("muslim","talaq"))).count()

  val bjpMuslimPositiveCount = filteredDataFrameOnBjp.filter(row=>sentenceContainsWord(row.getString(0),positiveWords)).filter(row=>sentenceContainsWord(row.getString(0),Array("muslim","talaq"))).count()

  println("Percentage of talking negatively for BJP on muslim is : " + (bjpMuslimNegativeCount.toFloat/totalBjpCount)*100)

  println("Percentage of talking positively for BJP on muslim is : " + (bjpMuslimPositiveCount.toFloat/totalBjpCount)*100)


  val bjpDemonitizationNegativeCount = filteredDataFrameOnBjp.filter(row=>sentenceContainsWord(row.getString(0),negativeWords)).filter(row=>sentenceContainsWord(row.getString(0),Array("demonetization","cash","atm"))).count()

  val bjpDemonitizationPositiveCount = filteredDataFrameOnBjp.filter(row=>sentenceContainsWord(row.getString(0),positiveWords)).filter(row=>sentenceContainsWord(row.getString(0),Array("demonetization","cash","atm"))).count()

  println("Percentage of talking negatively for BJP on demonetization is : " + (bjpDemonitizationNegativeCount.toFloat/totalBjpCount)*100)

  println("Percentage of talking positively for BJP on demonetization is : " + (bjpDemonitizationPositiveCount.toFloat/totalBjpCount)*100)


  val filteredDataFrameOnSpCongress = dataFrameTweets.filter(lower($"tweet").contains("akhileshyadav")
    || lower($"tweet").contains("sp congress")
    || lower($"tweet").contains("spcong")
    || lower($"tweet").contains("akhilesh")
    || lower($"tweet").contains("dimple yadav")
    || lower($"tweet").contains("dimpleyadav")
    || lower($"tweet").contains("akhilesh yadav")
    || lower($"tweet").contains("rahul gandhi")
    || lower($"tweet").contains("rahulgandhi")
    || lower($"tweet").contains("samajwadi party")
    || lower($"tweet").contains("samajwadiparty")
    || lower($"tweet").contains("mulayam")).select("tweet")

  val filteredDataFrameOnSpCongressCount = filteredDataFrameOnSpCongress.count()

  println("Total people tweeting for SP / Congress : " + filteredDataFrameOnSpCongressCount)

  val spCongressPositiveCount = filteredDataFrameOnSpCongress.filter(row=>sentenceContainsWord(row.getString(0),positiveWords)).count()

  println("Total people talking positively for SP / Congress : " + spCongressPositiveCount)

  val spCongressNegativeCount = filteredDataFrameOnSpCongress.filter(row=>sentenceContainsWord(row.getString(0),negativeWords)).count()

  println("Total people talking negatively for SP / Congress : " + spCongressNegativeCount)


  val totalSpCount = spCongressPositiveCount+spCongressNegativeCount;
  val percentageOfSpCongressTalkingPositively = (spCongressPositiveCount.toFloat/totalSpCount)*100

  println("Percentage of talking positively for Sp/Congress is : " + percentageOfSpCongressTalkingPositively)


  val percentageOfSpCongressTalkingNegatively = (spCongressNegativeCount.toFloat/totalSpCount)*100

  println("Percentage of talking negatively for Sp/Congress is : " + percentageOfSpCongressTalkingNegatively)



  val bjpPercentageTalking = (totalBjpCount.toFloat/ (totalSpCount + totalBjpCount)) *100

  println("Percentage of talking for BJP is : " + bjpPercentageTalking)

  val spPercentageTalking = (totalSpCount.toFloat/ (totalSpCount + totalBjpCount)) *100

  println("Percentage of talking for SP is : " + spPercentageTalking)


  def sentenceContainsWord(sentence: String, words: Array[String]): Boolean = {
    val wordsInSentence = sentence.split(" ")
    val result = wordsInSentence.exists(words contains _)
    result;
  }
}