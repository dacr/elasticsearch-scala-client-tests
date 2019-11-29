package fr.janalyse.elasticsearch.client.scala.helpers

import com.sksamuel.elastic4s.ElasticClient
import fr.janalyse.elasticsearch.client.scala.ElasticClientHelper
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, Matchers}

abstract class ElasticClientTestsHelper extends AsyncFlatSpec with Matchers with BeforeAndAfterAll with ElasticClientHelper {
  var client: ElasticClient = _

  override def beforeAll: Unit = {
    client = getElasticClient()
  }

  override def afterAll(): Unit = {
    client.close()
  }

}
