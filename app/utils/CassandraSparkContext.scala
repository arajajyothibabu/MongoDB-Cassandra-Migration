package utils

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jyothi on 11/12/16.
  */
trait CassandraSparkContext {

  /**
    *
    * @param host hostName (optional if default host is used)
    * @return
    */
  def getCassandraSparkContext(host: String = "127.0.0.1"): SparkContext = {
    val remoteHost = "spark://192.168.0.21:4040"
    val conf = new SparkConf(true)
      .setMaster("local[*]")
      //.setMaster(remoteHost) //giving remote host for sparkJobs for JVM limitation
      .setAppName("SparkCassandraScalaPlay")
      .set("spark.app.id", "SparkCassandraScalaPlay")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.cassandra.connection.host", host) //we can set cassandra server name here

    val sc = new SparkContext(conf)
    sc
  }

}
