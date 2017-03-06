package kafkaproducer
import configreader.KafkaConfig
import models.{Tweet}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter.TwitterHelper
/**
  * Created by Satya on 03/12/2016.
  */
object TwitterStreamingProducer extends App {
  val apiKey = "6a4bp4dYU9wmNqXGmYIg4UFj0"
  val apiSecret = "W0juLHHw9vzBuTaMiFG9e6td8IdIcVd30vSGHd9gt8dbcMmmUW"
  val accessToken="43551820-PthPJRcBeAJXZeFOAnUe68sF5Da5ESM6oI0xabF32"
  val accessTokenSecret ="x10G2lr7Quy9G8XoKUgyxW3idJbjDIFBjAzEzLPA5xIS0"
  val tweetFilter = Array("BJP","Modi",
    "UP election",
    "NarendraModi",
    "Narendra Modi",
    "Amit Shah",
    "aap",
    "aamaadmi",
    "aam-aadmi",
    "kejriwal",
    "arvindkejriwal",
    "arvind",
    "AmitShah",
    "demonitisation",
    "Ganga",
    "UmaBharti",
    "Uma Bharti",
    "mayavati",
    "bsp maya",
    "dalit",
    "rss/bjp",
    "rss-bjp",
    "yadav sp",
    "antinational",
    "anti-national",
    "samajwadi party",
    "samajwadiparty",
    "Yogi Adityanath",
    "YogiAdityanath",
    "RajnathSingh",
    "Rajnath Singh",
    "SP Congress",
    "SP Cong",
    "spcong",
    "Banaras",
    "varanasi",
    "UP election win",
    "UP vote",
    "Rahul Gandhi",
    "RahulGandhi",
    "DimpleYadav",
    "Dimple Yadav",
    "Akhilesh",
    "Akhilesh Yadav",
    "AkhileshYadav",
    "yadav vote")
  val kafkaTwitterProducer = new KafkaTwitterProducer(KafkaConfig.topic,KafkaConfig.hostName + ":" + KafkaConfig.port)
  private val intervalSecs = 5

  startProducingTweets()

  def startProducingTweets(): Unit ={

    val conf = new SparkConf(true)
    val sc = new SparkContext("local[5]", "example", conf)
    TwitterHelper.configureTwitterCredentials(apiKey, apiSecret, accessToken, accessTokenSecret)
    val ssc = new StreamingContext(sc, Seconds(intervalSecs))
    val tweets = TwitterUtils.createStream(ssc, None,tweetFilter)
    var localTweets = tweets.filter(x=> x.getUser != null && x.getText !=null && !x.getText.isEmpty
      ).map(result =>
      {
             if(result.getGeoLocation() == null)
               {
                 Tweet(result.getUser().getName(),0,
                   0,result.getText(),result.getCreatedAt())
               }
        else {
          Tweet(result.getUser().getName(), result.getGeoLocation().getLatitude(),
            result.getGeoLocation().getLongitude(), result.getText(), result.getCreatedAt())
        }

      })
    localTweets = localTweets.filter(x=>x.tweet!=null && !x.tweet.isEmpty)
    localTweets.foreachRDD(rdd=>{
      rdd.collect().foreach(x=>{
          println("Sending tweet to topic" + x.tweet)
          kafkaTwitterProducer.sendTweetToTopic(x)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
