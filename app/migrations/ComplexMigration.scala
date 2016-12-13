package migrations

import com.datastax.spark.connector.SomeColumns
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bson.Document
import com.datastax.spark.connector._
import utils.HybridSparkContext

/**
  * Created by jyothi on 12/12/16.
  */
object ComplexMigration extends HybridSparkContext{

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

    println("getting Hybrid Spark Context")
    val sparkContext = getHybridSparkContext(db, collection) // uses default host

    println("Retrieving data from Mongo...")
    val users = MongoSpark.load(sparkContext)

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

    val tableColumns = SomeColumns(
      "id",
      "sdk_version",
      "app_bundle_id",
      "app_name",
      "app_version",
      "app_mode",
      "device_uuid",
      "custom_user_id",
      "hardware_model",
      "os_version",
      "device_height",
      "device_width",
      "countryid",
      "langid",
      "stateid"
    )

    try {
      saveRDDToCassandra(users, sparkContext, keySpace, table, tableColumns)
    } catch {
      case e: Exception => Seq[String](); println("Exception raised....> " + e)
      case _ => println("****************Other Exception*****************")
    }
    println("Saving RDD to cassandra table...")

    val cassandraUsers = sparkContext.cassandraTable(keySpace, table)

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
  def saveRDDToCassandra(mongoRDD: MongoRDD[Document], sc: SparkContext, keySpace: String, table: String, tableColumns: SomeColumns) {

    val emptyDocument = Document.parse("{}")
    mongoRDD.map(document => { //FIXME: need elegant code here to reduce this boilerplate
      val appInfo = document.getOrDefault("app_info", emptyDocument).asInstanceOf[Document]
      val deviceInfo = document.getOrDefault("device_info", emptyDocument).asInstanceOf[Document]
      val userInfo = document.getOrDefault("user_info", emptyDocument).asInstanceOf[Document]
      val deviceDimensions = deviceInfo.getOrDefault("device_dimensions", emptyDocument).asInstanceOf[Document]
      (
        document.getString("_id"),
        document.getDouble("sdk_version").toFloat,
        appInfo.getString("bundle_id"),
        appInfo.getString("app_name"),
        appInfo.getString("app_version"),
        appInfo.getString("app_mode"),
        deviceInfo.getString("device_uuid"),
        deviceInfo.getString("custom_user_id"),
        deviceInfo.getString("hardware_model"),
        deviceInfo.getString("os_version"),
        deviceDimensions.getInteger("height"),
        deviceDimensions.getInteger("width"),
        userInfo.getString("countryId"),
        userInfo.getString("langId"),
        userInfo.getString("stateId")
      )
    }).saveToCassandra(keySpace, table, tableColumns)
  }

}
