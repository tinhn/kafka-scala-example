# Kafka example for Scala

Example code for connecting to Apache Kafka cluster and authenticate with SSL_SASL and SCRAM. This demonstration have use CloudKarafka cluster service 


### Configure

All of the authentication settings can be config first, something like:

```
BROKERS=broker1:9094,broker2:9094,broker3:9094
USERNAME=<username>
PASSWORD=<password>
```

### Build

```
git clone
cd kafka-scala-example
mvn clean compile assembly:single
```
The application build and store at dist folder with name <kafka-scala-example_2.0.0-1.0.jar>

You can start a Java application that pushes messages to Kafka in one Thread and read messages in the main Thread. 
The output you will see in the terminal is the messages received in the consumer.
### Running application
The application can be store a jar file in dist folder. Now you can start the java application for pushes or receive message from/ to Kafka.

## Send data
```
cd dist
java -cp kafka-scala-example_2.0.0-1.0.jar tinhn.kafka.training.ProducerExample <brokers> <topics> <username> <password>
```