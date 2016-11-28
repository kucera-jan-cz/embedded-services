package com.github.embedded.kafka

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.typesafe.config.Config
import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
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

  def getProducer[K, V](): Producer[String, String] = {
    val server = Await.result(serverInit, Duration.Inf)
    logger.debug("Server {}", server.correlationId)
    val props = new Properties()

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.ACKS_CONFIG, "1")
    props.put(ProducerConfig.RETRIES_CONFIG, "1")
    val producer = new KafkaProducer[String, String](props, new StringSerializer, new StringSerializer)
    producer
  }

  def getConsumer[K, V](): Consumer[String, String] = {
    Await.ready(serverInit, Duration.Inf)
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "test")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    val consumer = new KafkaConsumer[String, String](props, new StringDeserializer, new StringDeserializer)
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
    kafkaProps.put(KafkaConfig.HostNameProp, "127.0.0.1")
    kafkaProps.put(KafkaConfig.AdvertisedHostNameProp, "127.0.0.1")
    kafkaProps.put(KafkaConfig.LogDirProp, "c:\\temp\\kafka")
    kafkaProps.put(KafkaConfig.BrokerIdProp, "0")
    kafkaProps.put(KafkaConfig.LogFlushSchedulerIntervalMsProp, "1")
    kafkaProps.put(KafkaConfig.NumPartitionsProp, "1")
    kafkaProps.put(KafkaConfig.AutoCreateTopicsEnableProp, "true")
    kafkaProps.put(KafkaConfig.PortProp, "9092")
    kafkaProps.put(KafkaConfig.LogFlushIntervalMsProp, "1000")
    kafkaProps.put(KafkaConfig.AutoLeaderRebalanceEnableProp, "true")
    val kafkaConfig = new KafkaConfig(kafkaProps)
    val kafkaServer = new KafkaServer(kafkaConfig)
    kafkaServer.startup()
    kafkaServer
  }

}
