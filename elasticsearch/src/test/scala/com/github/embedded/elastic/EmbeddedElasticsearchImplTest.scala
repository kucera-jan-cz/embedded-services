package com.github.embedded.elastic

import com.typesafe.config.{ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import org.elasticsearch.index.query.QueryBuilders
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

class EmbeddedElasticsearchImplTest extends FunSuite {
	private val logger = LoggerFactory.getLogger(classOf[EmbeddedElasticsearchImplTest])

	val config = ConfigFactory.load("elasticsearch.properties",
		ConfigParseOptions.defaults(),
		ConfigResolveOptions.noSystem())
	val elastic = new EmbeddedElasticsearchImpl(config)
	val client = elastic.getClient()

	test("insert dummy data") {
		logger.info("Status: {}", client.admin().cluster().prepareHealth().get().status().name())
		elastic.insertData("a", "data", "classpath:samples/dummy_data.txt")
		val response = client.prepareSearch("a").setQuery(QueryBuilders.matchAllQuery()) get()
		assert(response.getHits.totalHits() == 2)

		val hits = response.getHits.getHits
		val ids = hits.map(h => h.getSource.get("id").asInstanceOf[Int]).toList.sorted
		assert(ids == List(1, 2))

		elastic.deleteAll()
		assert(client.admin().indices().prepareGetIndex().get().indices().size == 0, "At least on index was not deleted")
	}

	test("index template") {
		val index = "t01_20160720"
		elastic.createTemplate("t01", "classpath:templates/t01.json")
		elastic.insertData(index, "data", "classpath:samples/dummy_data.txt")
		val indices = client.admin().indices()
		val mappingResponse = indices.prepareGetFieldMappings(index).setTypes("data").setFields("A", "B", "C", "id").get()
		val fields = mappingResponse.mappings().get(index).get("data").keys.toList.sorted
		assert(fields == List("A", "B", "C", "id"))
	}
}
