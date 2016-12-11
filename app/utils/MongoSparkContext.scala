package utils

import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jyothi on 11/12/16.
  */
trait MongoSparkContext {

  /**
    *
    * @param db Database Name
    * @param collection Collection Name
    * @param host Host name (optional)
    * @return
    */
  def getMongoSparkContext(db: String, collection: String, host: String = "localhost"): SparkContext = {
    val uri: String = s"mongodb://$host/$db.$collection"
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SparkMongoScalaPlay")
      .set("spark.app.id", "SparkMongoScalaPlay")
      .set("spark.mongodb.input.uri", uri)
      .set("spark.mongodb.output.uri", uri)
      .set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)
    MongoConnector(sc).withDatabaseDo(WriteConfig(sc), {db => /*db.drop()//drops the entire db*/}) //for bootstrapping if any
    sc
  }

}
