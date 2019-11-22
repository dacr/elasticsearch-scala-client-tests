package fr.janalyse.elasticsearch.client.scala

import com.sksamuel.elastic4s.ElasticDsl._

class BasicsTest extends ElasticClientTestsHelper {
  // ----------------------------------------------------------------------
  "elasticsearch client application" should "be able to get cluster state information" in {
    client.execute {
      clusterState()
    } map { response =>
      response.result.nodes.size should be > 0
    }
  }

}
