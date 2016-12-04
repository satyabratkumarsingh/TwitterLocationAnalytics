package kafkaproducer

import java.util.{Properties, UUID}
import java.util.Properties

import kafka.message.{DefaultCompressionCodec, NoCompressionCodec}
import models.Tweet
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArraySerializer
import serializer.TwitterByteArraySerializer



case class KafkaTwitterProducer(
                          topic: String,
                          brokerList: String,
                          clientId: String = UUID.randomUUID().toString,
                          synchronously: Boolean = true,
                          compress: Boolean = true,
                          batchSize: Integer = 200,
                          messageSendMaxRetries: Integer = 3,
                          requestRequiredAcks: Integer = -1
                        ) {

  val props = new Properties()
  val codec = if(compress) DefaultCompressionCodec.codec else NoCompressionCodec.codec
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.ByteArraySerializer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, Array[Byte]](props)


  def sendTweetToTopic(tweet:  Tweet): Unit ={
    try {
      val message = new ProducerRecord[String, Array[Byte]](topic, null, TwitterByteArraySerializer.serialize(tweet))
      producer.send(message)
      println("Message submitted")
    } catch {
      case e: Exception =>
        e.printStackTrace
        System.exit(1)
    }
   }
}
