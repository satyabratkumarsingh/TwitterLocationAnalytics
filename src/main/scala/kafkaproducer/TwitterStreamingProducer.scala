package kafkaproducer
import configreader.KafkaConfig
import models.{Location, Tweet}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter.TwitterHelper
import serializer.TwitterByteArraySerializer
import utilities.Utilities.StringUtilities
/**
  * Created by Satya on 03/12/2016.
  */
object TwitterStreamingProducer extends App {
  val apiKey = "6a4bp4dYU9wmNqXGmYIg4UFj0"
  val apiSecret = "W0juLHHw9vzBuTaMiFG9e6td8IdIcVd30vSGHd9gt8dbcMmmUW"
  val accessToken="43551820-PthPJRcBeAJXZeFOAnUe68sF5Da5ESM6oI0xabF32"
  val accessTokenSecret ="x10G2lr7Quy9G8XoKUgyxW3idJbjDIFBjAzEzLPA5xIS0"
  val tweetFilter = Array("Modi", "Kejri", "India")
 // val kafkaTwitterProducer = new KafkaTwitterProducer(KafkaConfig.topic,KafkaConfig.hostName + ":" + KafkaConfig.port)
  private val intervalSecs = 5

  startProducingTweets()

  def startProducingTweets(): Unit ={

    val conf = new SparkConf(true)
    val sc = new SparkContext("local[2]", "example", conf)
    TwitterHelper.configureTwitterCredentials(apiKey, apiSecret, accessToken, accessTokenSecret)
    val ssc = new StreamingContext(sc, Seconds(intervalSecs))
    val tweets = TwitterUtils.createStream(ssc, None,tweetFilter)
    var localTweets = tweets.filter(x=> x.getUser()!=null && x.getText()!=null
      && x.getGeoLocation()!=null).map(result =>
      {
             Tweet(result.getUser().getName(),
          result.getText(),new Location(result.getGeoLocation().getLatitude(),
            result.getGeoLocation().getLongitude()))

      })
    localTweets = localTweets.filter(x=>x.text!=null && !x.text.isEmpty)
    localTweets.foreachRDD(rdd=>{
      println(rdd.collect().foreach(x=>println(x.text)))
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def isNullOrEmpty(x: String) = x == null || x.isEmpty

}
