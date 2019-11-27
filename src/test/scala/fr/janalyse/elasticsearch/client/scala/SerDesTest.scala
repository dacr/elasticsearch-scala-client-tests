package fr.janalyse.elasticsearch.client.scala

import java.time.{Instant, OffsetDateTime, ZoneOffset}

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.json4s.ElasticJson4s.Implicits._
import com.sksamuel.elastic4s.requests.searches.AvgAggResult
import org.json4s.JValue
import org.scalatest.OptionValues._

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Success
import scala.util.Random.{nextDouble, nextInt, nextLong}

case class Address(town: String, country: String)

case class Someone(name: String, size: Double, birthDate: OffsetDateTime, address: Address)

class SerDesTest extends ElasticClientTestsHelper {

  def generateSomeone(): Someone = {
    val firstNames = List("sarah", "joe", "john")
    val firstName = firstNames(nextInt(firstNames.size))
    val lastName = "doe" + nextInt(1000)
    val name = s"$firstName $lastName"
    val size = 1.2d + nextDouble() * 2 / 3
    val choices = List(
      "paris" -> "france","rennes" -> "france",
      "abidjan" -> "cote d'ivoire", "boston" -> "us", "berlin"->"allemagne")
    val (town, country) = choices(nextInt(choices.size))
    val birthDate = Instant.ofEpochSecond(System.currentTimeMillis()/1000L - nextLong(60L*60*24*365*80))
    Someone(name, size, birthDate.atOffset(ZoneOffset.of("+01")), Address(town,country))
  }

  "someone generator" should "be able to create a good human" in {
    val someone = generateSomeone()
    info(someone.toString)
    someone.size shouldBe 1.2d +- 0.7d
    someone.name should include regex ".*doe.*"
    succeed
  }

  // ----------------------------------------------------------------------
  "elasticsearch client application" should "to insert a case class" in {
    val birthDate = OffsetDateTime.parse("2010-01-01T01:02:03Z")
    val address = Address("chicago", "us")
    val joe = Someone("joe", size = 1.85d, birthDate = birthDate, address = address)

    client.execute {
      indexInto("serdes").doc(joe).refreshImmediately
    }.map { result =>
      result.isSuccess shouldBe true
    }
  }

  it should "be possible to search and extract responses" in {
    client.execute {
      search("serdes").query("joe")
    }.map { response =>
      val people = response.result.to[Someone]
      people.size shouldBe 1
      people.headOption.value.name shouldBe "joe"
    }
  }

  it should "be possible to safely search and extract responses" in {
    client.execute {
      search("serdes").query("joe")
    }.map { response =>
      val people = response.result.safeTo[Someone].collect { case Success(x) => x }
      people.size shouldBe 1
      people.headOption.value.name shouldBe "joe"
    }
  }

  it should "be possible to write a bulk of documents" in {
    info("Simple way, just one bulk, so just 1 future")
    client.execute {
      bulk {
        for {
          _ <- 1 to 10000
          someone = generateSomeone()
        } yield {
          indexInto("serdes").doc(someone)
        }
      }
    }.map{response =>
      response.result.failures.size shouldBe 0
    }
  }

  def doScroll(scrollId: String, limit:Int, accu:IndexedSeq[Someone]=IndexedSeq.empty): Future[IndexedSeq[Someone]] = {
    client
      .execute { searchScroll(scrollId).keepAlive("1m")}
      .map {response => response.result.scrollId -> response.result.to[Someone]}
      .flatMap {
        case (Some(nextScrollId), scrolledPeople) if scrolledPeople.size > 0 =>
          doScroll(nextScrollId, limit, accu++scrolledPeople)
        case (_, scrolledPeople) =>
          Future.successful(accu++scrolledPeople)
      }
  }

  it should "be possible to read a large number of documents by steps" in {
    val limit = 100
    info(s"going to read response using a $limit window size")
    val response = for {
      refreshResponse <- client.execute{refreshIndex("serdes")}
      if refreshResponse.isSuccess
      response <- client.execute { search("serdes").scroll("1m").limit(limit)}
    } yield response.result.scrollId -> response.result.to[Someone]

    response
      .collect{case (Some(scrollId),firstPeople) => scrollId->firstPeople}
      .flatMap{ case (scrollId, people) => doScroll(scrollId, limit, people) }
      .map { people =>
        people.size shouldBe 10001
      }
  }

  it should "be possible to compute people average size" in {
    client.execute{
      search("serdes")
        .query{ queryStringQuery("joe") }
        .aggs {
          avgAgg("average", "size")
        }
    }.map { response =>
      //val result = response.result.aggregations.to[AvgAggResult]
      //result.value shouldBe 1.5d +- 0.5
      val result = response.result.aggregations.to[JValue]
      (result \\ "value").extract[Double] shouldBe 1.5d +- 0.5
    }
  }

  it should "be able to delete the index used for those tests" in {
    client.execute {
      deleteIndex("serdes")
    }.map { response =>
      response.isSuccess shouldBe true
    }
  }
}
