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

    val mongoCassandraKeyMapper = Map[String, String](
      "_id" -> "id",
      "sdk_version" -> "sdk_version",

      "app_info.bundle_id" -> "app_bundle_id",
      "app_info.app_name" -> "app_name",
      "app_info.app_version" -> "app_version",
      "app_info.app_mode" -> "app_mode",

      "device_info.device_uuid" -> "device_info.device_uuid",
      "device_info.custom_user_id" -> "custom_user_id",
      "device_info.hardware_model" -> "hardware_model",
      "device_info.os_version" -> "os_version",
      "device_info.device_dimensions.height" -> "device_height",
      "device_info.device_dimensions.width" -> "device_width",

      "user_info.countryId" -> "countryId",
      "user_info.langId" -> "langId",
      "user_info.stateId" -> "stateId"
    )

    val tableColumns = SomeColumns(mongoCassandraKeyMapper.values.toList.mkString(","))

    println("Converting MongoRDD to CassandraRDD with CassandraContext...")
    val usersRDD = cassandraRDDFromMongoRDD(users, cassandraSparkContext, mongoCassandraKeyMapper)

    println("Saving RDD to cassandra table...")
    usersRDD.saveToCassandra(keySpace, table, tableColumns)

    val cassandraSamples = cassandraSparkContext.cassandraTable(keySpace, table)

    println("****************From Cassandra Spark Context*****************")
    println("NO: of User Records inserted:: " + cassandraSamples.count())

    println("User Record:: " + cassandraSamples.first)

  }

  /**
    * Converts MongoRDD into Cassandra Context supporting RDD (Applicable for small length records)
    * @param mongoRDD MongoRDD
    * @param sc cassandra Context
    * @return
    */
  def cassandraRDDFromMongoRDD(mongoRDD: MongoRDD[Document], sc: SparkContext, keyMapper: Map[String, String]): RDD[(_)] = {

    val count = mongoRDD.count().toInt //converting to Int FIXME:
    val rddList = mongoRDD.toLocalIterator.toIndexedSeq //converting mongoRDD to Seq
    var document = rddList.head
    sc.parallelize(0 until count).map{ index =>
      document = rddList.apply(index)
      keyMapper.keySet.foreach(mongoKey => {
        document.get(mongoKey)
      })
    }

  }

}
