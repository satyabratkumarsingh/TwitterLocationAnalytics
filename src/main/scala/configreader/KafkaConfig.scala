package configreader

import java.io.File

import com.typesafe.config.ConfigFactory

/**
  * Created by Satya on 01/12/2016.
  */
object  KafkaConfig {

  private val config =
    ConfigFactory.parseFile(new File("F:\\BigData\\TwitterLocationAnalytics\\src\\main\\scala\\configs\\" + "Kafka.conf"))
  def hostName = {config.getString("host")}
  def port = {config.getString("port")}
  def topic = {config.getString("topic")}
  def zookeeperHost = {config.getString("zookeeperHost")}
}
