import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.datastax.spark.connector._
import com.google.gson.Gson
import org.apache.spark.streaming.twitter._


final case class Employee(EmployeeId:String,FirstName:String,LastName:String,Address:String)

object SparkMeApp {

  private var numTweetsCollected = 0L
  private var partNum = 0
  private var gson = new Gson()
  private val intervalSecs = 1
 case class Employee(country:String, employee_id:Int,email_id:String, first_name:String, last_name:String,middle_name:String);

 def main(args: Array[String]) {

    /*val sparkSession = SparkSession.builder.
      master("local")
      .appName("Satya's First Spark application").config("spark.sql.warehouse.dir","file:///c:/tmp/spark-warehouse")
      .getOrCreate()

    val dfAccounts = sparkSession.read.option("header","true").
      csv("F:\\BigData\\Files\\Accounts.csv")

    val dfEmployees = sparkSession.read.option("header","true").
      csv("F:\\BigData\\Files\\Employees.csv")

      val accountEmployees = dfAccounts.join(dfEmployees, dfAccounts("employee_id") === dfEmployees("EmployeeId"))

      import sparkSession.implicits._
      val dataSetEmployee =  dfEmployees.as[Employee]
      //val indianEmployees = dataSetEmployee.filter(_.Address.contains("India"))
     // println(indianEmployees.show());

      //println(accountEmployees.show())*/


   val sparkMasterHost = "192.168.0.6"
    val cassandraHost = "127.0.0.1"
    val keyspace = "sparkks"
    val table = "employee"
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", cassandraHost)
    val sc = new SparkContext("local[2]", "example", conf)
    //val cassandraSqlContext = new CassandraSQLContext()
    val rdd = sc.cassandraTable(keyspace, table).select("employee_id","first_name").as((_:Int,_:String))

    val apiKey = "6a4bp4dYU9wmNqXGmYIg4UFj0"
    val apiSecret = "W0juLHHw9vzBuTaMiFG9e6td8IdIcVd30vSGHd9gt8dbcMmmUW"
    val accessToken="43551820-PthPJRcBeAJXZeFOAnUe68sF5Da5ESM6oI0xabF32"
    val accessTokenSecret ="x10G2lr7Quy9G8XoKUgyxW3idJbjDIFBjAzEzLPA5xIS0"
    TwitterHelper.configureTwitterCredentials(apiKey, apiSecret, accessToken, accessTokenSecret)
    val ssc = new StreamingContext(sc, Seconds(intervalSecs))
   val tweets = TwitterUtils.createStream(ssc, None,Array("Modi", "Kejri", "India")).map(gson.toJson(_))
   /*tweets.foreachRDD((rdd, time) => {
     val count = rdd.count()
     if (count > 0) {
       val outputRDD = rdd.
       outputRDD.saveAsTextFile(outputDirectory + "/tweets_" + time.milliseconds.toString)
       numTweetsCollected += count
       if (numTweetsCollected > numTweetsToCollect) {
         System.exit(0)
       }
     }
   })*/
   ssc.start()
   ssc.awaitTermination()

      //.where("employee_id > ?",1).keyBy(row=> row.getString("country")).spanByKey

  println(rdd.sortByKey().collect().foreach(println))

    val col = sc.parallelize(Seq(Employee("UK",12,"hardeep.mahajan@gmail.com","Hardeep","K","Mahajan")));
   // println(col.collect().foreach(println))
    //col.saveToCassandra(keyspace, table)


   // sc.stop()
  }
}