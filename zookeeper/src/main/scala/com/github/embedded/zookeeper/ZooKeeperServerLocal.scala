package com.github.embedded.zookeeper

import java.io.File
import java.net.InetSocketAddress

import org.apache.zookeeper.server._
import org.slf4j.LoggerFactory

private[zookeeper] class ZooKeeperServerLocal(snapshotDir: File, logDir: File, port: Int) {
  private val logger = LoggerFactory.getLogger(classOf[ZooKeeperServerLocal])
  val zkServer = new ZooKeeperServer(logDir, logDir, 3000)
  val factory = new NIOServerCnxnFactory()

  def startUp(): Unit = {
    logger.info("Starting Zookeeper server")
    factory.configure(new InetSocketAddress("127.0.0.1", port), 100)
    factory.startup(zkServer)
    //    val txnLog = new FileTxnSnapLog(new File(config.getDataDir()), new File(config.getDataDir()))
    //    zkServer.setTxnLogFactory(txnLog)
    //    zkServer.setTickTime(config.getTickTime)
    //    zkServer.setMinSessionTimeout(config.getMinSessionTimeout)
    //    zkServer.setMaxSessionTimeout(config.getMaxSessionTimeout)
    //    factory.configure(config.getClientPortAddress(), config.getMaxClientCnxns())
    //    factory.startup(zkServer)
  }

  def shutdownNow(): Unit = {
    factory.shutdown()
    zkServer.shutdown()
  }
}
