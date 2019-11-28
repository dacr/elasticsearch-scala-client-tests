package fr.janalyse.elasticsearch.client.scala

import fr.janalyse.split.CsvSplit.split
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties, Response}
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.ElasticDsl.{count, _}
import com.sksamuel.elastic4s.json4s.ElasticJson4s.Implicits._
import com.sksamuel.elastic4s.requests.mappings._
import com.sksamuel.elastic4s.requests.mappings.FieldType._
import org.json4s.{DefaultFormats, native}
import org.json4s.ext.JavaTimeSerializers
import java.time.{Instant, OffsetDateTime, ZoneId}
import java.time.format.DateTimeFormatter

import com.sksamuel.elastic4s.requests.bulk.BulkResponse
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent._
import scala.concurrent.duration._
import annotation.tailrec

object ChicagoCrimesImport extends ElasticClientHelper {
  val logger = LoggerFactory.getLogger(getClass())

  import scala.concurrent.ExecutionContext.Implicits.global

  val indexName = "crimes"
  val mappingName = "testmapping"

  // Customize the default configuration, we're going to insert a huge amount of data in a unclean but fast way
  val client = getElasticClient()

  def now() = System.currentTimeMillis()

  def doCreateTestIndex(name: String) = client.execute {
    logger.info(s"doCreateTestIndex($name)")
    createIndex(name)
      .mappings {
        properties() as Seq(
          geopointField("location")
        )
      }
  }

  def doUpdateIndexRefreshInterval(name: String, value: String) = {
    logger.info(s"doUpdateIndexRefreshInterval($name, $value)")
    client.execute {
      updateIndexLevelSettings(name)
        .refreshInterval(value)
    }
  }
  def doLoadingOptimizationsStart(name: String) = {
    logger.info(s"doLoadingOptimizationsStart($name)")
    client.execute {
      updateIndexLevelSettings(name)
        .numberOfReplicas(0)
        .refreshInterval("-1")
    }
  }
  def doLoadingOptimizationEnd(name: String) = {
    logger.info(s"doLoadingOptimizationsEnd($name)")
    client.execute {
      updateIndexLevelSettings(name)
        .numberOfReplicas(1)
        .refreshInterval("10s")
    }
  }
  def doRefreshIndex(name: String) = {
    logger.info(s"doRefreshIndex($name)")
    client.execute { refreshIndex(name) }
  }
  def doClean(name: String) = {
    logger.info(s"doClean($name)")
    client.execute { deleteIndex(name) }
  }
  def doCount(name: String) = {
    logger.info(s"doCount($name)")
    client.execute { count(name) }
  }

  def insertBulk(name: String, entries: Seq[Map[String, String]]) = client.execute {
    print("*")
    bulk {
      for {entry <- entries} yield {
        indexInto(name ).doc(entry)
      }
    }
  }


  val dateFormat = DateTimeFormatter.ofPattern("MM/d/yyyy hh:mm:ss a").withZone(ZoneId.of("America/Chicago"))

  def normalizeDate(date: String): String = {
    Instant.from(dateFormat.parse(date)).toString
  }

  def normalizeHeaders(headers:List[String]):List[String] = {
    headers.map(_.replaceAll("""\s+""", ""))
  }

  def lineToCell(line:String, limit:Int):Array[String] = {
    line.split("""\s*,\s*""", limit)
  }

  def lineToDocument(headers:List[String])(line: String): Map[String, String] = {
    // Join headers and cells into a map
    val cells = headers.zip(split(line)).toMap
    // Convert date format and normalize Timezone
    val foundTimestamp =
      cells
        .get("Date")
        .map(normalizeDate)
    // remove parenthesis and space from geopoint and filter out missing locations
    val foundLocation =
      cells
        .get("Location")
        .map(_.replaceAll("""[^-,.0-9]""", ""))
        .filterNot(_.trim.size==0)
        .filter(_.matches("""-?\d+[.]\d+,-?\d+[.]\d+"""))
    // Build the final document map
    (cells -- Set("Date", "Location")) ++
      foundTimestamp.map(timestamp => "timestamp" -> timestamp) ++
      foundLocation.map(location => "location" -> location)
  }

  type WriteStatus = Response[BulkResponse]
  type WriteStatuses = List[WriteStatus]
  def fill() {
    val linesIterator = scala.io.Source.fromFile("crimes.csv").getLines
    val headers = normalizeHeaders(linesIterator.next.split("""\s*,\s*""").toList)

    def writeData(indexName: String): Future[WriteStatuses] = {
      logger.info(s"writeData($indexName)")
      def write(iterator: linesIterator.GroupedIterator[String], responses: WriteStatuses=Nil):Future[WriteStatuses] = {
        if (iterator.hasNext) {
          insertBulk(indexName, iterator.next.map(lineToDocument(headers)))
              .flatMap { response => write(iterator, response::responses)}
        } else Future.successful(responses)
      }
      def it: linesIterator.GroupedIterator[String] =
        linesIterator.grouped(1000) // .take(500) // for test purposes, when you want to limit inputs
      write(it) // TODO - check all results responses
    }


    val startedAt = now()

    val futureResponse = for {
      cleaned <- doClean(indexName) // delete any existing indexName
      created <- doCreateTestIndex(indexName) // create the indexName, required for geoloc type mapping
      refreshDisabledResponse <- doLoadingOptimizationsStart(indexName) // To accelerate insertion
      responses <- writeData(indexName) // bulk operation insert all events
      refreshEnabledResponse <- doLoadingOptimizationEnd(indexName) // revert back to a normal behavior
      refreshed <- doRefreshIndex(indexName) // to wait for every to be available for search...
      count <- doCount(indexName)
    } yield {
      count
    }


    Await.result(futureResponse, 30.minutes) // because we don't want to exit the script before the future has completed

    futureResponse map { countResponse =>
      val duration = (now() - startedAt) / 1000
      println(s"$countResponse documents inserted in $duration seconds")
    }
  }

  def main(args: Array[String]): Unit = {
    fill()
  }
}
