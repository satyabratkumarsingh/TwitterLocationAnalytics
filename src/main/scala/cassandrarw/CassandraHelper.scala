package cassandrarw

import models.Tweet
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import configreader.CassandraConfig

/**
  * Created by Satya on 27/11/2016.
  */
object CassandraHelper {

  def saveTweetInCassandra(tweet: Tweet): Unit = {
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", CassandraConfig.hostName)
    val sc = new SparkContext("local[2]", "example", conf)
    val col = sc.parallelize(Seq(tweet));
    col.saveToCassandra(CassandraConfig.keySpace, CassandraConfig.table)
  }
}
