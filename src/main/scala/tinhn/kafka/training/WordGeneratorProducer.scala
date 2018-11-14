package tinhn.kafka.training

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random

/**
  * java -cp kafka-scala-example_2.0.0-1.0.jar tinhn.kafka.training.WordGeneratorProducer <brokers> <topics> <username> <password> /temp/word-list.txt
  */
object WordGeneratorProducer {
  def main(args: Array[String]): Unit = {

    if (args.length < 5) {
      System.err.println(s"""
            |Usage: ProducerExample <brokers> <topics> <username> <password>
            |  <brokers> is a list of one or more Kafka brokers
            |  <topics> is a list of one or more kafka topics to consume from
            |  <username> is an account for authentication with kafka cluster
            |  <password> is a password of this account for authentication
            |  <filepath> is a full path file
            |
        """.stripMargin)
      System.exit(1)
    }

    //val brokers = scala.util.Try(args(0)).getOrElse("localhost:9092")
    val Array(brokers, topic, username, password, filepath) = args
    val events = 100
    val intervalEvent = 1
    val props = new MyKafkaConfig(username,password).GetKafkaProperties(brokers)

    val producer = new KafkaProducer[String, String](props)
    val wordList = Source.fromFile(filepath).getLines().toSeq

    println("======================BEGIN=============================")

    val beforeTime = System.currentTimeMillis()
    for (i <- Range(0, events)) {
      val rndWords = getRandomWordList(wordList)
      val key = i.toString()
      val value = rndWords.mkString(",")
      val data = new ProducerRecord[String, String](topic, key, value)

      println(data)
      producer.send(data)

      if (intervalEvent > 0)
        Thread.sleep(intervalEvent * 1000)
    }
    val afterTime = System.currentTimeMillis()

    println("======================END=============================")
    println("Total time: " + ((afterTime - beforeTime) / 1000) + " sec")
    producer.close()
  }

  def getRandomWordList(words: Seq[String]): Seq[String] = {

    var rdnWords = new ListBuffer[String]()
    val rnd = new Random()

    for (i <- 0 until 10) {
      rdnWords += words(rnd.nextInt(words.length))
    }

    rdnWords
  }
}
