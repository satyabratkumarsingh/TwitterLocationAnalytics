package cassandrarw
import models.Tweet
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import configreader.CassandraConfig

/**
  * Created by Satya on 27/11/2016.
  */
object CassandraHelper {


  def saveTweetInCassandra(tweet:Tweet): Unit = {
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", CassandraConfig.hostName)
    val sc = new SparkContext("local[2]", "example", conf)
    val rdd = sc.cassandraTable(CassandraConfig.keySpace, CassandraConfig.table).select("employee_id","first_name").as((_:Int,_:String))
    //val col = sc.parallelize(Seq(Tweet("UK",12,"hardeep.mahajan@gmail.com","Hardeep","K","Mahajan")));
    // println(col.collect().foreach(println))
    //col.saveToCassandra(keyspace, table)
  }
}
