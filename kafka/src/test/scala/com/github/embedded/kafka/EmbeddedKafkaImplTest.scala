package com.github.embedded.kafka


import java.util
import java.util.Properties

import com.github.embedded.zookeeper.EmbeddedZookeeperImpl
import com.typesafe.config.{ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}
import org.apache.kafka.common.security.JaasUtils
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory

class EmbeddedKafkaImplTest extends FunSuite {
  private val logger = LoggerFactory.getLogger(classOf[EmbeddedKafkaImplTest])

  val config = ConfigFactory.load("zookeeper.properties", ConfigParseOptions.defaults(), ConfigResolveOptions.noSystem())
  val zk = new EmbeddedZookeeperImpl(config)
  val connectionString = zk.getConnectionString()


  test("initialize and shutdown") {
//    val zkUtils = ZkUtils.apply(connectionString, 30000, 30000,JaasUtils.isZkSecurityEnabled())
//    AdminUtils.createTopic(zkUtils, "config", 1, 1, new Properties())


    val kafka = new EmbeddedKafkaImpl(connectionString, null)
    val producer: Producer[String, String] = kafka.getProducer()
    val topic = s"topic-${System.currentTimeMillis()}"
    val zkUtils = ZkUtils.apply(connectionString,6000, 6000, false)
//    AdminUtils.createTopic(zkUtils, topic, 1, 1)
    AdminUtils.createTopic(zkUtils, topic, 1, 1, new Properties())



    producer.send(new ProducerRecord(topic, "KEY", "VALUE")).get()

    val consumer: Consumer[String, String] = kafka.getConsumer()
    consumer.subscribe(util.Arrays.asList(topic))
    val records = consumer.poll(1000)
    assert(records.count() == 1)

    import collection.JavaConversions._
    assert(records.iterator().toList(0).value() == "VALUE")
  }


}
