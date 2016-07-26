package com.github.embedded.zookeeper

import java.nio.file.{Files, Path, Paths}
import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext.Implicits.global
import com.typesafe.config.Config
import org.apache.zookeeper.server.quorum.QuorumPeerConfig
import org.apache.zookeeper.server.{ServerConfig, ZooKeeperServer, ZooKeeperServerMain}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class EmbeddedZookeeperImpl(config: Config) extends EmbeddedZookeeper {
  private val logger = LoggerFactory.getLogger(classOf[EmbeddedZookeeperImpl])
  private val nodeFuture = Future(initCluster())

  override def getConnectionString(): String = {
    val server = Await.result(nodeFuture, Duration.Inf)
    val address = server.config.getClientPortAddress
    s"${address.getAddress.getHostAddress}:${address.getPort}"
  }

  override def shutdown(): Unit = {
    shutdownNow()
  }

  override def awaitTermination(timeout: Long, unit: TimeUnit): Unit = {
    Await.ready(shutdownNow(), Duration(timeout, unit))
  }

  override def awaitTermination(): Unit = {
    Await.ready(shutdownNow(), Duration.Inf)
  }

  def shutdownNow(): Future[Unit] = {
    nodeFuture.flatMap(server => Future {
      server.shutdownNow()
      logger.info("Embedded ZooKeeper server has been closed")
    })
  }

  private def initCluster(): ZooKeeperServerLocal = {
    logger.info("Starting Embedded ZooKeeper server ")
    val quorumConfig = new QuorumPeerConfig
    val properties = toProperties(config)
    val dataDir = createTempZookeeperDirectory("target/zk-data", "zk")
    properties.put("dataDir", dataDir.resolve("data").toFile.toString)
    properties.put("dataLogDir", dataDir.resolve("logs").toFile.toString)
    quorumConfig.parseProperties(properties)
    val serverConfig = new ServerConfig
    serverConfig.readFrom(quorumConfig)
    val server = new ZooKeeperServerLocal(serverConfig)
    server.startUp()
    server
  }

  private def createTempZookeeperDirectory(dataDirectory: String, instanceName: String): Path = {
    val parentDataDir = Paths.get(dataDirectory)
    Files.createDirectories(parentDataDir)
    val dataDir = Files.createTempDirectory(parentDataDir, s"${instanceName}__")
    logger.debug("Creating temporary Zookeeper dir: {}", dataDir.toAbsolutePath)
    Files.createDirectories(dataDir.resolve(dataDirectory))
    dataDir
  }

  private def toProperties(config: Config): Properties = {
    val props = new Properties()
    import scala.collection.JavaConversions._
    val map: Map[String, Object] = config.entrySet().map(e => e.getKey -> e.getValue.unwrapped())(collection.breakOut)

    props.putAll(map)
    props
  }
}
