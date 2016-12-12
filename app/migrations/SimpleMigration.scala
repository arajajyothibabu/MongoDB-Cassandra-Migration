package migrations

import com.mongodb.spark.MongoSpark
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bson.Document
import utils.{CassandraSparkContext, MongoSparkContext}
import com.datastax.spark.connector._
import com.mongodb.spark.rdd.MongoRDD

/**
  * Created by jyothi on 11/12/16.
  */
/**
  * Sample Migration with sample  data
  */
object SimpleMigration extends MongoSparkContext with CassandraSparkContext{

  def main(args: Array[String]): Unit = {
    val db = "jyothi"

    /**
      * samples schema: {_id: String, sample_number: Integer}
      */
    val collection = "samples"

    println("getting Mongo Spark Context")
    val mongoSparkContext = getMongoSparkContext(db, collection) // uses default host

    println("getting Cassandra Spark Context")
    val cassandraSparkContext = getCassandraSparkContext() //uses default host

    println("Saving random data to mongo...")
    MongoSpark.save(getRandomRecords(mongoSparkContext, 1000)) // saving 1000 random documents

    println("Retrieving data from Mongo...")
    val samples = MongoSpark.load(mongoSparkContext)

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

    println("Converting MongoRDD to SparkRDD with CassandraContext...")
    val samplesRDD = cassandraRDDFromMongoRDD(samples, cassandraSparkContext)

    println("Saving RDD to cassandra table...")
    samplesRDD.saveToCassandra(keySpace, table, SomeColumns("id", "sample_number"))

    val cassandraSamples = cassandraSparkContext.cassandraTable(keySpace, table)

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
    val rddList = mongoRDD.toLocalIterator.toList //converting mongoRDD to List
    var document = rddList.head
    sc.parallelize(0 until count).map{ index =>
      document = rddList.apply(index.toInt)
      (document.getString("_id"), document.getInteger("sample_number"))
    }

  }

}
