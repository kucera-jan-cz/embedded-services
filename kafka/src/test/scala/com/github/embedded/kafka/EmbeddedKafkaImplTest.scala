package com.github.embedded.kafka


import java.util

import com.github.embedded.zookeeper.EmbeddedZookeeperImpl
import com.typesafe.config.{ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import kafka.producer.{KeyedMessage, Producer}
import org.apache.kafka.clients.consumer.Consumer
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory

class EmbeddedKafkaImplTest extends FunSuite {
  private val logger = LoggerFactory.getLogger(classOf[EmbeddedKafkaImplTest])

  val config = ConfigFactory.load("zookeeper.properties", ConfigParseOptions.defaults(), ConfigResolveOptions.noSystem())
  val zk = new EmbeddedZookeeperImpl(config)
  val connectionString = zk.getConnectionString()
  val kafka = new EmbeddedKafkaImpl(connectionString, null)

  test("initialize and shutdown") {
    val producer: Producer[String, String] = kafka.getProducer()
    val topic = s"topic-${System.currentTimeMillis()}"
    producer.send(new KeyedMessage(topic, "KEY", "VALUE"))

    val consumer: Consumer[String, String] = kafka.getConsumer()
    consumer.subscribe(util.Arrays.asList(topic))
    val records = consumer.poll(1000)
    assert(records.count() == 1)

    import collection.JavaConversions._
    assert(records.iterator().toList(0).value() == "VALUE")
  }


}
