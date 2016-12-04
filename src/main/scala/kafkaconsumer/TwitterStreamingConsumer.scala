import cassandrarw.CassandraHelper
import configreader.CassandraConfig
import models.Tweet
import com.datastax.spark.connector.streaming._
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import serializer.TwitterByteArraySerializer
import com.datastax.spark.connector._

/**
  * Created by Satya on 04/12/2016.
  */
object TwitterStreamingConsumer extends App{

  val topic = "mytopic"
  val zkhosts = "x.x.x.x"
  val zkports = "2181"
  val brokerPath = "/brokers"

  startKafkaConsumer()

  def startKafkaConsumer() = {
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", CassandraConfig.hostName).
      setMaster("local[2]").setAppName("KafkaConsumer")
    conf.set("spark.driver.allowMultipleContexts", "true")
    val streamingContext = new StreamingContext(conf, Seconds(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer ],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("twitterTopic")
    val stream = KafkaUtils.createDirectStream[String, Array[Byte]](
      streamingContext,
      PreferConsistent,
      Subscribe[String, Array[Byte]](topics, kafkaParams)
    )
    stream
      .map(x=>
        {
         TwitterByteArraySerializer.deserialize(x.value()).asInstanceOf[Tweet]
        }).saveToCassandra(CassandraConfig.keySpace, CassandraConfig.table)

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
