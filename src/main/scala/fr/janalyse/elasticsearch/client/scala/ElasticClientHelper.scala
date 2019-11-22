package fr.janalyse.elasticsearch.client.scala

import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import com.sksamuel.elastic4s.http.JavaClient
import org.json4s.ext.JavaTimeSerializers
import org.json4s.{DefaultFormats, native}

trait ElasticClientHelper {
  implicit val serialization = native.Serialization
  implicit val formats = DefaultFormats.lossless ++ JavaTimeSerializers.all

  def getElasticClient(elasticPort:Int = 9201) = {
    ElasticClient(JavaClient(ElasticProperties(s"http://127.0.0.1:$elasticPort")))
  }
}
