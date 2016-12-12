package migrations

import com.datastax.spark.connector.SomeColumns
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD
import migrations.SimpleMigration.{getCassandraSparkContext, getMongoSparkContext}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bson.Document
import com.datastax.spark.connector._

/**
  * Created by jyothi on 12/12/16.
  */
object ComplexMigration {

  def main(args: Array[String]): Unit = {
    val db = "jyothi"

    /**
      * users schema: {
            "_id" : String,
            "sdk_version" : 1.0,
            "app_info" : {
                "bundle_id" : String,
                "app_name" : String,
                "app_version" : String,
                "app_mode" : String
            },
            "device_info" : {
                "device_uuid" : String,
                "custom_user_id" : String,
                "hardware_model" : String,
                "os_version" : String,
                "device_dimensions" : {
                    "height" : Int,
                    "width" : Int
                }
            },
            "user_info" : {
                "countryId" : String,
                "langId" : String,
                "stateId" : String
            }
        }
      */
    val collection = "users"

    println("getting Mongo Spark Context")
    val mongoSparkContext = getMongoSparkContext(db, collection) // uses default host

    println("getting Cassandra Spark Context")
    val cassandraSparkContext = getCassandraSparkContext() //uses default host

    println("Retrieving data from Mongo...")
    val users = MongoSpark.load(mongoSparkContext)

    println("****************From Mongo Spark Context*****************")
    println("NO: of User Records:: " + users.count())

    println("User Record:: " + users.first.toJson)

    //Migration of samples collection to Cassandra Samples Table
    val keySpace = "jyothi"
    /**
      * //Schema is FLATTEN STRUCTURE in Cassandra Table
      * CREATE TABLE users(
      *   id text PRIMARY KEY,
      *   sdk_version float,
      *   -_-_-_-_-_-_-_-_-_--_-_-_- APP_INFO
      *   app_bundle_id text,
      *   app_name text,
      *   app_version text,
      *   app_mode text,
      *   -_-_-_-_-_-_-_-_-_--_-_-_- DEVICE_INFO
      *   device_uuid text,
      *   custom_user_id text,
      *   hardware_model text,
      *   os_version text,
      *   device_height int,
      *   device_width int,
      *   -_-_-_-_-_-_-_-_-_--_-_-_- USER_INFO
      *   countryId text,
      *   langId text,
      *   stateId text
      *
      * )
      */
    val table = "users"


    val mongoCassandraKeyMapper = Map[String, String]( //FIXME: use it properly
      "_id" -> "id",
      "sdk_version" -> "sdk_version",

      "app_info.bundle_id" -> "app_bundle_id",
      "app_info.app_name" -> "app_name",
      "app_info.app_version" -> "app_version",
      "app_info.app_mode" -> "app_mode",

      "device_info.device_uuid" -> "device_uuid",
      "device_info.custom_user_id" -> "custom_user_id",
      "device_info.hardware_model" -> "hardware_model",
      "device_info.os_version" -> "os_version",
      "device_info.device_dimensions.height" -> "device_height",
      "device_info.device_dimensions.width" -> "device_width",

      "user_info.countryId" -> "countryId",
      "user_info.langId" -> "langId",
      "user_info.stateId" -> "stateId"
    )

    val tableColumns = SomeColumns("id", "sdk_version", "app_bundle_id", "app_name",  "app_version", "app_mode", "device_uuid","custom_user_id",  "hardware_model", "os_version", "device_height", "device_width", "countryid", "langid", "stateid")

    println("Converting MongoRDD to CassandraRDD with CassandraContext...")
    try {
      cassandraRDDFromMongoRDD(users, cassandraSparkContext).saveToCassandra(keySpace, table, tableColumns)
    } catch {
      case e: Exception => Seq[String]()
        println("Exception raised....> " + e)
      case _ => println("****************Exception*****************")
    }
    println("Saving RDD to cassandra table...")

    val cassandraUsers = cassandraSparkContext.cassandraTable(keySpace, table)

    println("****************From Cassandra Spark Context*****************")
    println("NO: of User Records inserted:: " + cassandraUsers.count())

    println("User Record:: " + cassandraUsers.first)

  }

  /**
    * Converts MongoRDD into Cassandra Context supporting RDD (Applicable for small length records)
    * @param mongoRDD MongoRDD
    * @param sc cassandra Context
    * @return
    */
  def cassandraRDDFromMongoRDD(mongoRDD: MongoRDD[Document],
                               sc: SparkContext):
  RDD[(String, Float, String, String, String, String, String, String, String, String, Int, Int, String, String, String)] = {

    val count = mongoRDD.count().toInt //converting to Int FIXME:
    val rddList = mongoRDD.collect() //converting mongoRDD to Array
    var document = rddList.head
    sc.parallelize(0 until count).map{ index =>
      document = rddList.apply(index)
      println("doc" + document.toJson)
      ( //FIXME: need elegant code here to reduce this boilerplate
        document.getString("_id"),
        document.getDouble("sdk_version").toFloat,
        document.getString("app_info.bundle_id"),
        document.getString("app_info.app_name"),
        document.getString("app_info.app_version"),
        document.getString("appInfo.app_mode"),
        document.getString("device_info.device_uuid"),
        document.getString("device_info.custom_user_id"),
        document.getString("device_info.hardware_model"),
        document.getString("device_info.os_version"),
        document.getInteger("device_info.device_dimensions.height"),
        document.getInteger("device_info.device_dimensions.width"),
        document.getString("user_info.countryId"),
        document.getString("user_info.langId"),
        document.getString("user_info.stateId")
      )
    }

  }

}
