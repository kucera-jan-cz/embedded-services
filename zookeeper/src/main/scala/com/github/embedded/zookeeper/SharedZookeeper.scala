package com.github.embedded.zookeeper

import org.scalatest.{BeforeAndAfterAll, FunSuite}

trait SharedZookeeper extends BeforeAndAfterAll {
  self: FunSuite =>
  val zk: EmbeddedZookeeper = SharedZookeeper.zk

  override def afterAll(): Unit = {
    try {
      zk.deleteAll()
    } finally {
      super.afterAll()
    }
  }

  object SharedZookeeper {
    val zk = new EmbeddedZookeeperImpl(ZookeeperConfig.config)
  }

}
