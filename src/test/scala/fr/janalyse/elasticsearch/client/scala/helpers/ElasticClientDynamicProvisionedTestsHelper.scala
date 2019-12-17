package fr.janalyse.elasticsearch.client.scala.helpers

import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs
import org.elasticsearch.common.settings.Settings.Builder
import org.json4s.ext.JavaTimeSerializers
import org.json4s.{DefaultFormats, native}
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, Matchers}

abstract class ElasticClientDynamicProvisionedTestsHelper extends AsyncFlatSpec with Matchers with BeforeAndAfterAll {
  implicit val serialization = native.Serialization
  implicit val formats = DefaultFormats.lossless ++ JavaTimeSerializers.all

  val elasticPort = 10201
  val elasticProperties = ElasticProperties(s"http://127.0.0.1:$elasticPort")

  val runner = new ElasticsearchClusterRunner()
  var client: ElasticClient = _

  override def beforeAll: Unit = {
    val configs =
      newConfigs
        .numOfNode(1)
        .basePath("elastic-data")
    runner.onBuild(
      new ElasticsearchClusterRunner.Builder() {
        override def build(number:Int, settingsBuilder:Builder):Unit = {
          settingsBuilder.put("cluster.routing.allocation.disk.threshold_enabled", false)
          settingsBuilder.put("node.name", s"Node#$number")
          settingsBuilder.put("http.port", 10200+number)
          settingsBuilder.put("transport.port", "10300-10400")
        }
      }
    ).build(configs)
    runner.ensureYellow()
    client = ElasticClient(JavaClient(elasticProperties))
  }

  override def afterAll(): Unit = {
    client.close()
    runner.close()
    runner.clean()
  }

}
