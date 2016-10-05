package com.github.embedded.kafka

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.typesafe.config.Config
import kafka.producer.{Producer, ProducerConfig}
import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.kafka.clients.consumer.{Consumer, KafkaConsumer}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class EmbeddedKafkaImpl(zkConnection: String, config: Config) extends EmbeddedKafka {
  private val logger = LoggerFactory.getLogger(classOf[EmbeddedKafkaImpl])
  private val serverInit = Future(initServer())

  override def deleteAll(): Unit = {}

  override def shutdown(): Unit = {
    shutdownNow()
  }

  override def awaitTermination(timeout: Long, unit: TimeUnit): Unit = {
    Await.ready(shutdownNow(), Duration(timeout, unit))
  }

  override def awaitTermination(): Unit = {
    Await.ready(shutdownNow(), Duration.Inf)
  }

  private def shutdownNow(): Future[Unit] = {
    logger.info("Embedded Kafka {} has been closed")
    Future(shutdown())
  }

  def getProducer[K, V](): Producer[K, V] = {
    val server = Await.result(serverInit, Duration.Inf)
    logger.debug("Server {}", server.correlationId)
    val props = new Properties()
    props.put("metadata.broker.list", "0.0.0.0:9092")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "100")
    props.put("request.required.acks", "1")
    val producer = new Producer[K, V](new ProducerConfig(props))
    producer
  }

  def getConsumer[K, V](): Consumer[K, V] = {
    Await.ready(serverInit, Duration.Inf)
    val props = new Properties()
    props.put("bootstrap.servers", "0.0.0.0:9092")
    props.put("group.id", "test")
    props.put("enable.auto.commit", "false")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val consumer = new KafkaConsumer[K, V](props)
    consumer
  }

  def getConnection(): Properties = {
    val smap = Map("" -> "")
    null
  }

  private def initServer(): KafkaServer = {
    logger.info("Starting Embedded Kafka server...")
    val kafkaProps = new Properties()
    kafkaProps.put(KafkaConfig.ZkConnectProp, zkConnection)
    val kafkaConfig = new KafkaConfig(kafkaProps)
    val kafkaServer = new KafkaServer(kafkaConfig)
    kafkaServer.startup()
    kafkaServer
  }

}
