package com.github.embedded.elastic

import com.typesafe.config.ConfigFactory

object ElasticConfig {
	val default = ConfigFactory.load(this.getClass.getClassLoader, "elasticsearch.properties")
	val config = ConfigFactory.parseResources("elasticsearch.properties").withFallback(default)
	val x = List(1,2).view.force
}
