package tinhn.kafka.training

import java.text.SimpleDateFormat
import java.util.{Calendar, UUID}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.util.control.Breaks
import scala.util.Random

/**
  * java -cp kafka-scala-example_2.0.0-1.0.jar tinhn.kafka.training.ProducerExample <brokers> <topics> <username> <password>
  */
object ProducerExample {
  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      System.err.println(s"""
          |Usage: ProducerExample <brokers> <topics> <username> <password>
          |  <brokers> is a list of one or more Kafka brokers
          |  <topics> is a list of one or more kafka topics to consume from
          |  <username> is an account for authentication with kafka cluster
          |  <password> is a password of this account for authentication
          |
        """.stripMargin)
      System.exit(1)
    }

    //val brokers = scala.util.Try(args(0)).getOrElse("localhost:9092")
    val Array(brokers, topic, username, password) = args

    val events = 0
    val intervalEvent = 30
    val rndStart = 50
    val rndEnd = 1200 //in second
    val props = new MyKafkaConfig(username,password).GetKafkaProperties(brokers)

    val producer = new KafkaProducer[String, String](props)

    println("======================BEGIN=============================")

    val car_data_source = Seq("Ford", "Mazda", "KIA", "Toyota", "Mecedez-Ben", "BMW","AUDI")
    val rnd = new Random()
    val rnd2 = new Random()

    var i = 0
    val loop = new Breaks()

    loop.breakable {
      while (true) {

        val n = rndStart + rnd2.nextInt(rndEnd - rndStart + 1)
        for (i <- Range(0, n)) {

          val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val key = UUID.randomUUID().toString().split("-")(0)
          val value = formatter.format(Calendar.getInstance.getTime) + "," + car_data_source(rnd.nextInt(car_data_source.length))
          val data = new ProducerRecord[String, String](topic, key, value)

          producer.send(data)
        }

        val k = i + 1
        println(s"--- #$k: $n records in [$rndStart, $rndEnd] ---")

        if (intervalEvent > 0)
          Thread.sleep(intervalEvent * 1000)

        i += 1
        if (events > 0 && i == events)
          loop.break()
      }
    }
  }
}
