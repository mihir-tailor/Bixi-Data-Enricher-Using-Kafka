package ca.mcit.bigdata.sprint3

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}

import scala.io.Source

object Producer extends App {
  val topicName = "bdsf1901_mihir_trip"
  val producerProperties = new Properties()
  producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName)
  producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  val producer = new KafkaProducer[Int, String](producerProperties)

  var i : Int = 0
  val dataPath = "/home/bd-user/sprint3_data/"
  val dataSource = Source.fromFile(dataPath + "100_trips.csv")
  dataSource
    .getLines()
    .foreach(line => {
      i += 1
      producer.send(new ProducerRecord[Int,String](topicName,i,line))
    })
  dataSource.close()
}
