package ca.mcit.bigdata.sprint3

import ca.mcit.bigdata.sprint3.model.TripSchema
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Consumer extends App {

  val spark = SparkSession.builder()
    .master("local[*]").appName("Spark streaming with Kafka for Enrichment")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

  val kafkaConfig = Map[String, String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.GROUP_ID_CONFIG -> "test",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
  )

  val topic = "bdsf1901_mihir_trip"
  val inStream: DStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](List(topic), kafkaConfig)
  )

  val enrichedStationInformation = "hdfs://quickstart.cloudera/user/fall2019/mihir/enriched_station_information/"
  val enrichedStationDf = spark.read
    .option("header", "true")
    .csv(enrichedStationInformation)

  val enrichedStationDfSchema = enrichedStationDf.select(
    col("system_id").as("system_id"),
    col("timezone").as("timezone"),
    col("station_id").as("station_id"),
    col("station_name").as("name"),
    col("short_name").as("start_station_code"),
    col("lat").as("lat"),
    col("lon").as("lon"),
    col("capacity").as("capacity")
  )

  inStream.map(_.value()).foreachRDD(rdd => businessLogic(rdd))

  ssc.start()
  ssc.awaitTermination()

  println("=======================\nStation Visualization at: https://tabsoft.co/34Xl9DI")

  def businessLogic(rdd: RDD[String]): Unit = {
    import spark.implicits._
    val trip: RDD[TripSchema] = rdd.map(csvTrip => TripSchema(csvTrip))
    val tripDf = trip.toDF()
    if (tripDf.isEmpty) {
      println("No new message received")
    }
    else {
      println("Message received from: " + topic + " is enriched with enriched_station_information")
      enrichedStationDfSchema
        .join(tripDf, "start_station_code")
        .coalesce(1)
        .write.mode(SaveMode.Overwrite).option("header", "false")
        .csv("hdfs://quickstart.cloudera/user/fall2019/mihir/sprint3/enriched_trips/")
      println("Enriched_trips.csv created & uploaded to HDFS.")
    }
  }
}