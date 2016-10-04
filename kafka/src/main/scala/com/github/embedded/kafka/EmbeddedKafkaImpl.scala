package com.github.embedded.kafka

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.typesafe.config.Config
import kafka.server.{KafkaConfig, KafkaServer}
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

class EmbeddedKafkaImpl(zkConnection: String, config: Config) extends EmbeddedKafka {
	private val logger = LoggerFactory.getLogger(classOf[EmbeddedKafkaImpl])
	private val serverInit = Future(initServer)

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
	}

	private def initServer(): KafkaServer = {
		logger.info("Starting Embedded Kafka server...")
		val kafkaProps = new Properties()
		val kafkaConfig = new KafkaConfig(kafkaProps)
		val kafkaServer = new KafkaServer(kafkaConfig)
		kafkaServer.startup()
		kafkaServer
	}

}
