package fr.janalyse.elasticsearch.client.scala

import fr.janalyse.split.CsvSplit.split
import com.sksamuel.elastic4s.ElasticDsl.{count, _}
import com.sksamuel.elastic4s.json4s.ElasticJson4s.Implicits._
import com.sksamuel.elastic4s.requests.mappings._
import com.sksamuel.elastic4s.requests.mappings.FieldType._
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter

import org.slf4j.LoggerFactory
import scala.concurrent._

object ChicagoCrimesImport extends ElasticClientHelper {
  val logger = LoggerFactory.getLogger(getClass())

  import scala.concurrent.ExecutionContext.Implicits.global

  val indexName = "crimes"
  val mappingName = "testmapping"

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

  case class BulksStatus(successes:Int, failures:Int)
  def fill() {
    val linesIterator = scala.io.Source.fromFile("crimes.csv").getLines
    val headers = normalizeHeaders(linesIterator.next.split("""\s*,\s*""").toList)

    // quite faster but can be improved because of the "rendez-vous" effect
    def writeDataPar(indexName: String): Future[BulksStatus] = {
      logger.info(s"writeData($indexName)")
      // TODO take care, recursive and not tail rec
      def write(iterator: linesIterator.GroupedIterator[String], status: BulksStatus=BulksStatus(0,0)):Future[BulksStatus] = {
        if (iterator.hasNext) {
          val groupsParallelProcessingFutures = iterator.take(30).map { group =>
            insertBulk(indexName, group.map(lineToDocument(headers)))
          }
          val groupsParallelProcessingFuture = Future.sequence(groupsParallelProcessingFutures)
          groupsParallelProcessingFuture.flatMap{ groupsResponses =>
            val successes = groupsResponses.filter(_.isSuccess).size
            val failures = groupsResponses.size - successes
            val nextStatus = BulksStatus(status.successes + successes,status.failures+failures)
            write(iterator,nextStatus)
          }
        } else Future.successful(status)
      }
      write(linesIterator.grouped(2000)) // TODO - check all results status
    }

    // Slow
    def writeDataSeq(indexName: String): Future[BulksStatus] = {
      logger.info(s"writeData($indexName)")
      def write(iterator: linesIterator.GroupedIterator[String], status: BulksStatus=BulksStatus(0,0)):Future[BulksStatus] = {
        if (iterator.hasNext) {
          insertBulk(indexName, iterator.next.map(lineToDocument(headers)))
              .flatMap { response =>
                val nextStatus =
                  if (response.isSuccess) BulksStatus(status.successes + 1,status.failures)
                  else BulksStatus(status.successes,status.failures+1)
                write(iterator, nextStatus)
              }
        } else Future.successful(status)
      }
      write(linesIterator.grouped(1000)) // TODO - check all results status
    }

    val startedAt = now()

    val futureResponse = for {
      cleaned <- doClean(indexName) // delete any existing indexName
      created <- doCreateTestIndex(indexName) // create the indexName, required for geoloc type mapping
      refreshDisabledResponse <- doLoadingOptimizationsStart(indexName) // To accelerate insertion
      //response <- writeDataSeq(indexName) // bulk operation insert all events
      response <- writeDataPar(indexName) // bulk operation insert all events
      refreshEnabledResponse <- doLoadingOptimizationEnd(indexName) // revert back to a normal behavior
      refreshed <- doRefreshIndex(indexName) // to wait for every to be available for search...
      count <- doCount(indexName)
    } yield {
      println(response) // bulks operation status
      count
    }

    futureResponse map { countResponse =>
      val duration = (now() - startedAt) / 1000
      println(s"$countResponse documents inserted in $duration seconds")
    }
  }

  def main(args: Array[String]): Unit = {
    val response = fill()
    //Await.result(futureResponse, 30.minutes) // because we don't want to exit the script before the future has completed
  }
}
