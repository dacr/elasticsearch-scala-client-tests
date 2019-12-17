package fr.janalyse.elasticsearch.client.scala

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.requests.searches.TermsAggResult
import fr.janalyse.elasticsearch.client.scala.helpers.{ElasticClientDynamicProvisionedTestsHelper, ElasticClientTestsHelper}
import org.json4s.Extraction
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.scalatest.OptionValues._
import scala.concurrent.duration._

class ChicagoCrimesTest extends ElasticClientDynamicProvisionedTestsHelper {

  override def beforeAll(): Unit = {
    super.beforeAll
    val result = ChicagoCrimesImport(elasticProperties).importCSV(limitOption=Some(10000))
    result.await(10.minutes)
  }
  // ----------------------------------------------------------------------
  "elasticsearch client application with chicago crimes index" should "be able to count the total number of crimes" in {
    val countFuture = client.execute {
      count("crimes")
    }.map(_.result.count)
    countFuture.map{ count =>
      info("Found "+count)
//      count shouldBe 7000000L +- 100000L
      count shouldBe 10000L
    }
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
      info("Found "+response.result.hits.total.value)
//      response.result.hits.total.value shouldBe 5000L +- 1000L
      response.result.hits.total.value shouldBe 1L
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
      response.result.aggregations.cardinality("crimesCountByType").value shouldBe 27d
    }
  }


  // ----------------------------------------------------------------------

  case class Bucket(key:String, doc_count:Int)

  it should "be possible to count how many crimes for each primary type" in {
    val responseResult = client.execute {
      search("crimes").matchAllQuery().aggs {
        termsAgg("primaryTypesAgg", "PrimaryType.keyword").size(26)
      }
    }
    responseResult.map{response=>
      val rawresults = Extraction.decompose(response.result.aggregations.data.get("primaryTypesAgg"))
      val buckets = (rawresults \ "buckets").extract[Array[Bucket]]
      val results = buckets.map(bucket => bucket.key -> bucket.doc_count).toMap
      results.size shouldBe 26
      results.get("NARCOTICS").value shouldBe 1225
    }
    /*
    responseResult.map{response=>
      val termsAggResult = Extraction.decompose(response.result.aggregations).extract[TermsAggResult]
      val results = termsAggResult.buckets.map(b => b.key->b.docCount).toMap
      results.get("NARCOTICS").value shouldBe 724752
    }
    */
  }

}
