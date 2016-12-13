package migrations

import com.mongodb.spark.MongoSpark
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bson.Document
import utils.{CassandraSparkContext, HybridSparkContext, MongoSparkContext}
import com.datastax.spark.connector._
import com.mongodb.spark.rdd.MongoRDD

/**
  * Created by jyothi on 11/12/16.
  */
/**
  * Sample Migration with sample  data
  */
object SimpleMigration extends HybridSparkContext{

  def main(args: Array[String]): Unit = {
    val db = "jyothi"

    /**
      * samples schema: {_id: String, sample_number: Integer}
      */
    val collection = "samples"

    println("getting Hybrid Spark Context")
    val sparkContext = getHybridSparkContext(db, collection) // uses default host

    println("Saving random data to mongo...")
    MongoSpark.save(getRandomRecords(sparkContext, 1000)) // saving 1000 random documents

    println("Retrieving data from Mongo...")
    val samples = MongoSpark.load(sparkContext)

    println("****************From Mongo Spark Context*****************")
    println("NO: of Sample Records inserted:: " + samples.count())

    println("Sample Record:: " + samples.first.toJson)

    //Migration of samples collection to Cassandra Samples Table
    val keySpace = "jyothi"
    /**
      * @ query to create table
      * samples table Schema
      * CREATE TABLE samples (
      *   id text PRIMARY KEY,
      *   sample_number int
      * )
      */
    val table = "samples"
    val tableColumns  = SomeColumns("id", "sample_number")

    println("Converting MongoRDD to CassandraRDD with CassandraContext...")
    val samplesRDD = cassandraRDDFromMongoRDD(samples, sparkContext)

    println("Saving RDD to cassandra table...")
    samplesRDD.saveToCassandra(keySpace, table, tableColumns)

    val cassandraSamples = sparkContext.cassandraTable(keySpace, table)

    println("****************From Cassandra Spark Context*****************")
    println("NO: of Sample Records inserted:: " + cassandraSamples.count())

    println("Sample Record:: " + cassandraSamples.first)

  }

  /**
    *
    * @param sc SparkContext
    * @param count count of records to be generated
    * @return
    */
  def getRandomRecords(sc: SparkContext, count: Int): RDD[Document] = {

    println("Generating random records...")
    val randomDocuments = sc.parallelize((1 to count).map(i => Document.parse(s"{_id: 'sample-${Math.abs(scala.util.Random.nextInt())}', sample_number: $i}")))
    println("Generating random records done.!")
    randomDocuments

  }

  /**
    * Converts MongoRDD into Cassandra Context supporting RDD (Applicable for small length records)
    * @param mongoRDD MongoRDD
    * @param sc cassandra Context
    * @return
    */
  def cassandraRDDFromMongoRDD(mongoRDD: MongoRDD[Document], sc: SparkContext): RDD[(String, Int)] = {

    val count = mongoRDD.count().toInt //converting to Int
    val rddList = mongoRDD.collect() //converting mongoRDD to Array
    var document = rddList.head
    sc.parallelize(0 until count).map{ index =>
      document = rddList.apply(index)
      (document.getString("_id"), document.getInteger("sample_number"))
    }

  }

}
