package fr.janalyse.elasticsearch.client.scala

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.json4s.ElasticJson4s.Implicits._
import fr.janalyse.elasticsearch.client.scala.helpers.ElasticClientDynamicProvisionningTestsHelper


class BasicsTest extends ElasticClientDynamicProvisionningTestsHelper {
  // ----------------------------------------------------------------------
  "elasticsearch client application" should "be able to get cluster state information" in {
    client.execute {
      clusterState()
    }.map { response =>
      response.result.nodes.size should be > 0
      response.result.clusterName.size should be > 0
    }
  }

  it should "be able to create an index" in {
    client.execute {
      createIndex("basics").shards(3).replicas(1)
    }.map { response =>
      response.isSuccess shouldBe true
    }
  }

  it should "be able to append a new document" in {
    client.execute {
      indexInto("basics")
        .doc(Map("msg"->"Hello World"))
    }.map {response =>
      response.isSuccess shouldBe true
    }
  }

  it should "be able to append a new document with a specified id" in {
    client.execute {
      indexInto("basics")
        .id("42")
        .doc(Map("msg"->"Hello World"))
    }.map {response =>
      response.isSuccess shouldBe true
    }
  }

  it should "be able to update an existing document" in {
    client.execute {
      update("42")
        .in("basics")
        .doc(Map("msg"->"Hello World !!"))
    }.map {response =>
      response.isSuccess shouldBe true
    }
  }

  it should "be able to get a document" in {
    client.execute {
      get("42").from("basics")
    }.map {response =>
      response.result.source.size should be > 0
      response.result.version shouldBe 2
    }
  }

  it should "be able to update/INSERT a document" in {
    info("If the document doesn't exist it is inserted, if it exists then it is updated")
    client.execute {
      update("4242")
        .in("basics")
        .docAsUpsert(Map("msg"-> "Bonjour tout le monde !"))
    }.map {response =>
      response.isSuccess shouldBe true
    }
  }

  it should "be able to UPDATE/insert a document" in {
    info("If the document doesn't exist it is inserted, if it exists then it is updated")
    client.execute {
      update("4242")
        .in("basics")
        .docAsUpsert(Map("msg"-> "Bonjour tout le monde !!"))
    }.map {response =>
      response.isSuccess shouldBe true
    }
  }

  it should "be able to refresh explicitly an index" in {
    info("Take care of performance, this is not something you should do")
    client.execute {
      refreshIndex("basics")
    }.map{ response =>
      response.isSuccess shouldBe true
    }
  }

  it should "be able to count documents" in {
    client.execute {
      count("basics")
    }.map {response =>
      response.result.count shouldBe 3L
    }
  }

  it should "be able to delete an index" in {
    client.execute {
      deleteIndex("basics")
    }.map { response =>
      response.isSuccess shouldBe true
    }
  }
}
