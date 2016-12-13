package utils

import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jyothi on 13/12/16.
  */
trait HybridSparkContext {

  /**
    *
    * @param db mongoDB name
    * @param collection mongoDB collection
    * @param host server host for mongoDB and Cassandra
    * @return
    */
  def getHybridSparkContext(db: String, collection: String, host: String = "127.0.0.1"): SparkContext = {
    val mongoDBUri: String = s"mongodb://$host/$db.$collection"
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("HybridSparkContext")
      .set("spark.app.id", "HybridSparkContext")
      .set("spark.mongodb.input.uri", mongoDBUri)
      .set("spark.mongodb.output.uri", mongoDBUri)
      .set("spark.cassandra.connection.host", host) //we can set cassandra server name here
      //.set("spark.driver.allowMultipleContexts", "true") //allows multiple contexts

    val sc = new SparkContext(conf)
    MongoConnector(sc).withDatabaseDo(WriteConfig(sc), {db => /*db.drop()//drops the entire db*/}) //for bootstrapping if any
    sc
  }

}
