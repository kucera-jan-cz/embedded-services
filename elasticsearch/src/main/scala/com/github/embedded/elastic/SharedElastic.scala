package com.github.embedded.elastic

import org.scalatest.{BeforeAndAfterAll, FunSuite}

trait SharedElastic extends BeforeAndAfterAll {
	self: FunSuite =>
	val elastic: EmbeddedElasticsearch = SharedElastic.elastic

	override def beforeAll(): Unit = {
		super.beforeAll()
	}

	override def afterAll(): Unit = {
		SharedElastic.elastic.deleteAll()
		super.afterAll()
	}

	object SharedElastic {
		val elastic = new EmbeddedElasticsearchImpl(ElasticConfig.config)
	}

}
