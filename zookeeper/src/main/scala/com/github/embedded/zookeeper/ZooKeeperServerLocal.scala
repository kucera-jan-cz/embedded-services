package com.github.embedded.zookeeper

import java.io.File

import org.apache.zookeeper.server.persistence.FileTxnSnapLog
import org.apache.zookeeper.server.{ServerCnxnFactory, ServerConfig, ZooKeeperServer, ZooKeeperServerMain}
import org.slf4j.LoggerFactory

private[zookeeper] class ZooKeeperServerLocal(val config: ServerConfig) {
  private val logger = LoggerFactory.getLogger(classOf[ZooKeeperServerLocal])
  val zkServer = new ZooKeeperServer()
  val factory = ServerCnxnFactory.createFactory()

  def startUp(): Unit = {
    logger.info("Starting Zookeeper server")
    val txnLog = new FileTxnSnapLog(new File(config.getDataDir()), new File(config.getDataDir()))
    zkServer.setTxnLogFactory(txnLog)
    zkServer.setTickTime(config.getTickTime)
    zkServer.setMinSessionTimeout(config.getMinSessionTimeout)
    zkServer.setMaxSessionTimeout(config.getMaxSessionTimeout)
    factory.configure(config.getClientPortAddress(), config.getMaxClientCnxns())
    factory.startup(zkServer)
  }

  def shutdownNow(): Unit = {
    factory.shutdown()
  }
}
