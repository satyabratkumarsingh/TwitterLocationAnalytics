package configreader

import java.io.File

import com.typesafe.config.ConfigFactory

/**
  * Created by Satya on 28/11/2016.
  */
object CassandraConfig {
  private val config =
    ConfigFactory.parseFile(new File("F:\\BigData\\TwitterLocationAnalytics\\src\\main\\scala\\configs\\" + "Cassandra.conf"))
    def hostName = {config.getString("host")}
    def keySpace = {config.getString("keyspace")}
    def table = {config.getString("table")}
}
