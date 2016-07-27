package com.github.embedded.zookeeper

import com.typesafe.config.{ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
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

    assert(curator.checkExists().forPath(path) != null, "Path does not exists")

    val response = curator.getData().watched().forPath(path)
    assertResult(value.getBytes)(response)
    printChildren(curator, "/")
    printChildren(curator, "/zookeeper")
    zk.deleteAll()

    assert(curator.checkExists().forPath(path) == null)

    zk.awaitTermination()
  }

  private def printChildren(curator: CuratorFramework, path: String): Unit = {
    val allPathsResponse = curator.getChildren().forPath(path)
    logger.info("Children {}: {}", Array(path, allPathsResponse):_*)
  }
}
