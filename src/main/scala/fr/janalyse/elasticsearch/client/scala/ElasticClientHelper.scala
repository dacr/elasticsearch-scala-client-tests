package fr.janalyse.elasticsearch.client.scala

import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import com.sksamuel.elastic4s.http.JavaClient

trait ElasticClientHelper {
  def getElasticClient(elasticPort:Int = 9201) = {
    ElasticClient(JavaClient(ElasticProperties(s"http://127.0.0.1:$elasticPort")))
  }
}
