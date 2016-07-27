package com.github.embedded.zookeeper

import com.typesafe.config.ConfigFactory

object ZookeeperConfig {
	val default = ConfigFactory.load(this.getClass.getClassLoader, "zookeeper.properties")
	val config = ConfigFactory.parseResources("zookeeper.properties").withFallback(default)
}
