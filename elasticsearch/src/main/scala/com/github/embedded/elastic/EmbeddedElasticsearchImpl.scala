package com.github.embedded.elastic

import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.github.embedded.utils.io.ResourceUtils
import com.typesafe.config.Config
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{Client, ClusterAdminClient, IndicesAdminClient}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.node.Node
import org.elasticsearch.node.NodeBuilder._
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class EmbeddedElasticsearchImpl(config: Config) extends EmbeddedElasticsearch {
	private val logger = LoggerFactory.getLogger(classOf[EmbeddedElasticsearchImpl])
	private val clusterName: String = config.getString(ElasticConstants.CLUSTER_NAME)
	private val dataDirectory: String = config.getString(ElasticConstants.DATA_DIR)
	private val nodeFuture = Future(initCluster())
	private val mapper = new ObjectMapper()

	override def getClient(): Client = {
		val node = Await.result(nodeFuture, Duration.Inf)
		assert(!node.isClosed, "Node is closed")
		node.client()
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

	override def getClusterName: String = {
		clusterName
	}

	override def deleteAll(): Unit = {
		val indexDeleteResponse = iClient().prepareDelete("_all").get()
		assert(indexDeleteResponse.isAcknowledged, "Deletion of all indices failed")

		import scala.collection.JavaConversions._
		val templateNames = iClient().prepareGetTemplates().get().getIndexTemplates.map(_.name())
		val templateResponses = templateNames.map {
			iClient().prepareDeleteTemplate(_).execute()
		}
		assert(templateResponses.forall(_.get().isAcknowledged), "Deletion of templates failed")
	}

	def createIndex(name: String): Unit = {
		createIndex(name, None)
	}

	def createIndex(name: String, mappingPath: String): Unit = {
		createIndex(name, Some(mappingPath))
	}

	def createIndex(name: String, mappingPath: Option[String]): Unit = {
		val mappingAsText = mappingPath.map(ResourceUtils.asText(_)).getOrElse("{}")
		val client = iClient()
		val response = client.prepareCreate(name).setSource(mappingAsText).get()
		assert(response.isAcknowledged, s"Creating index $name failed")
	}

	def createTemplate(name: String, templatePath: String): Unit = {
		val templateAsText = ResourceUtils.asText(templatePath)
		val client = iClient()
		val response = client.preparePutTemplate(name).setSource(templateAsText).get()
		assert(response.isAcknowledged, s"Creating index template $name failed")
	}

	def insertData(index: String, indexType: String, resource: String): Unit = {
		val client = getClient()
		val bulk = client.prepareBulk()
		readJsonDocuments(resource).foreach(
			json => {
				val jsonAsText = mapper.writeValueAsString(json)
				val request = new IndexRequest(index, indexType).source(jsonAsText)
				bulk.add(request)
			}
		)
		val response = bulk.setRefresh(true).get()
		assert(!response.hasFailures, s"Failed to insert all documents: ${response.buildFailureMessage()}")
	}

	def readJsonDocuments(resource: String): Iterator[JsonNode] = {
		val is = ResourceUtils.asInputStream(resource)
		val factory = mapper.getFactory
		val parser = factory.createParser(is)
		import collection.JavaConverters._
		val it = parser.readValuesAs(classOf[JsonNode]).asScala
		it
	}

	private def initCluster(): Node = {
		logger.info("Starting Embedded Elasticsearch server {}", clusterName)
		import ElasticConstants._
		val dataDir = createTempElasticDirectory(dataDirectory, clusterName)
		import scala.collection.JavaConversions._
		import scala.collection.JavaConverters._
		val cfgAsProperties = config.entrySet().map(entry => entry.getKey -> entry.getValue.unwrapped().toString).toMap.asJava
		val settings = Settings.settingsBuilder()
			.put(cfgAsProperties)
			.put(PATH_HOME_PROP, dataDir.toString)
			.put(PATH_DATA_PROP, dataDir.resolve(DATA_DIR_NAME).toString)
			.build()

		val node = nodeBuilder().settings(settings).node()
		waitForStatus(node.client())
		node
	}

	def shutdownNow(): Future[Unit] = {
		nodeFuture.flatMap(n => Future {
			n.close()
			logger.info("Embedded Elasticsearch {} has been closed", clusterName)
		})
	}

	private def waitForStatus(client: Client, status: ClusterHealthStatus = ClusterHealthStatus.YELLOW): Unit = {
		client.admin().cluster().prepareHealth().setWaitForStatus(status).get()
	}

	private def createTempElasticDirectory(dataDirectory: String, clusterName: String): Path = {
		logger.debug("Creating temporary ES dir: {}", dataDirectory)
		val parentDataDir = Paths.get(dataDirectory)
		Files.createDirectories(parentDataDir)
		val dataDir = Files.createTempDirectory(parentDataDir, s"${clusterName}__")
		Files.createDirectories(dataDir.resolve(dataDirectory))
		dataDir
	}

	private def iClient(): IndicesAdminClient = {
		getClient().admin().indices()
	}

	private def cClient(): ClusterAdminClient = {
		getClient().admin().cluster()
	}
}
