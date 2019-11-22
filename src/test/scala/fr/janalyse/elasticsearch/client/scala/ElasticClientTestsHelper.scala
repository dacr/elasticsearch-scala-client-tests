package fr.janalyse.elasticsearch.client.scala

import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
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
