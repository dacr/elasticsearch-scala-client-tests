package fr.janalyse.elasticsearch.client.scala

import com.sksamuel.elastic4s.ElasticDsl._

class ChicagoCrimesTest extends ElasticClientTestsHelper {

  // ----------------------------------------------------------------------
  "elasticsearch client application with chicago crimes index" should "be able to count the total number of crimes" in {
    val countFuture = client.execute {
      count("crimes")
    }.map(_.result.count)
    countFuture.map{ count => count should be > 6000000L}
  }

  // ----------------------------------------------------------------------
  it should "be able to find homicides without arrest" in {
    val responseFuture = client.execute {
      search("crimes")
        .query {
          must(
            termQuery("Arrest", false),
            termQuery("PrimaryType", "homicide")
          )
        }
    }
    responseFuture.map{response =>
      response.result.hits.total.value should be > 0L
    }
  }

  // ----------------------------------------------------------------------
  it should "be possible to count the number of distinct primary types" in {
    val responseFuture = client.execute {
      search("crimes").aggs {
        cardinalityAgg("crimesCountByType", "PrimaryType.keyword")
      }
    }
    responseFuture.map{response =>
      response.result.aggregations.cardinality("crimesCountByType").value shouldBe 35d
    }
  }


  // ----------------------------------------------------------------------
  it should "be possible to count how many crimes for each primary type" in {
    val responseResult = client.execute {
      search("crimes").matchAllQuery().aggs {
        termsAgg("primaryTypesAgg", "PrimaryType.keyword").size(40)
      }
    }
    responseResult.map{response=>
      val rawresults = response.result.aggregations.data
      println(rawresults)
      rawresults.collect{case (primaryType,count:Long) => primaryType->count}
    }.map{results =>
      results.size shouldBe 35
      results.get("NARCOTICS") shouldBe > (0)
    }
  }

}
