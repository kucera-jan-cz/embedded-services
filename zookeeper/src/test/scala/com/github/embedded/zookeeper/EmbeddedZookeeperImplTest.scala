package com.github.embedded.zookeeper

import com.typesafe.config.{ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import org.apache.curator.RetryPolicy
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryOneTime
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory

import scala.util.Random



class EmbeddedZookeeperImplTest extends FunSuite {
  private val logger = LoggerFactory.getLogger(classOf[EmbeddedZookeeperImplTest])

  val config = ConfigFactory.load("zookeeper.properties", ConfigParseOptions.defaults(), ConfigResolveOptions.noSystem())
  val zk = new EmbeddedZookeeperImpl(config)
  val connectionString = zk.getConnectionString()

  test("initialize and shutdown") {
    val curator = CuratorFrameworkFactory.builder()
      .connectString(connectionString)
      .retryPolicy(new RetryOneTime(100))
      .build()
    curator.start()

    val path = "/test-ID"
    val value = new Random().nextString(16)
    curator.create().forPath(path, value.getBytes)
    val response = curator.getData().watched().forPath(path)
    assertResult(value.getBytes)(response)

    zk.awaitTermination()
  }

}
