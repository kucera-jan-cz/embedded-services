import com.github.embedded.elastic.SharedElastic
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory

class ElasticsearchSuite extends FunSuite with SharedElastic {
	private val logger = LoggerFactory.getLogger(classOf[ElasticsearchSuite])
	test("Starting elastic") {
		logger.info("Elasticsearch cluster name: {}", this.elastic.getClusterName)
	}
}
