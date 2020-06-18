package uk.co.gresearch.spark.dgraph.connector

import com.google.protobuf.ByteString
import io.dgraph.DgraphClient
import io.dgraph.DgraphProto.Mutation
import io.grpc.ManagedChannel
import org.scalatest.FunSpec
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.dgraph.DgraphTestCluster
import uk.co.gresearch.spark.dgraph.connector.sources.TestTriplesSource

class TestTransaction extends FunSpec with SparkTestSession with DgraphTestCluster {

  import spark.implicits._

  // we want a fresh cluster that we can mutate, definitively not one that is always running and used by all tests
  override val clusterAlwaysStartUp: Boolean = true

  describe("Connector") {

    def mutate(): Unit = {
      val channels: Seq[ManagedChannel] = Seq(toChannel(Target(cluster.grpc)))
      try {
        val client: DgraphClient = getClientFromChannel(channels)

        val mutationInsert = Mutation.newBuilder()
          .setSetNquads(ByteString.copyFromUtf8(
            s"""
               |_:1 <dgraph.type> "Person" .
               |_:1 <name> "Obi-Wan 'Ben' Kenobi" .
               |<0x${sw1.toHexString}> <starring> _:1 .
               |""".stripMargin
          ))
          .setCommitNow(true).build()

        val mutationUpdate = Mutation.newBuilder()
          .setSetNquads(ByteString.copyFromUtf8(
            s"""<0x${leia.toHexString}> <name> "Princess Leia Organa" .""".stripMargin
          ))
          .setCommitNow(true).build()

        val mutationDelete = Mutation.newBuilder()
          .setDelNquads(ByteString.copyFromUtf8(
            s"""<0x${sw1.toHexString}> <starring> <0x${leia.toHexString}> .""".stripMargin
          ))
          .setCommitNow(true).build()

        val transactionInsert = client.newTransaction()
        transactionInsert.mutate(mutationInsert)
        transactionInsert.close()

        val transactionUpdate = client.newTransaction()
        transactionUpdate.mutate(mutationUpdate)
        transactionUpdate.close()

        val transactionDelete = client.newTransaction()
        transactionDelete.mutate(mutationDelete)
        transactionDelete.close()
      } finally {
        channels.foreach(_.shutdown())
      }
    }

    it("should read in transaction") {
      val before = spark.read.dgraphTriples(cluster.grpc)
      val beforeTriples = before.as[TypedTriple].collect().toSet
      TestTriplesSource.doAssertTriples(beforeTriples, this)

      mutate()
      val afterBeforeTriples = before.as[TypedTriple].collect().toSet
      assert(beforeTriples === afterBeforeTriples)

      val after = spark.read.dgraphTriples(cluster.grpc)
      val afterTriples = after.as[TypedTriple].collect().toSet

      val expectedAfterTriples =
        beforeTriples
          // minus updated
          .filterNot(t => t.subject == leia && t.predicate == "name")
          // minus deleted
          .filterNot(t => t.subject == sw1 && t.predicate == "starring" && t.objectUid.contains(leia)) ++
          // plus
          Seq(
            // plus updated
            beforeTriples
              .find(t => t.subject == leia && t.predicate == "name")
              .map(_.copy(objectString = Some("Princess Leia Organa")))
              .get,
            // plus insterted
            TypedTriple(highestUid + 1, "dgraph.type", None, Some("Person"), None, None, None, None, None, None, "string"),
            TypedTriple(highestUid + 1, "name", None, Some("Obi-Wan 'Ben' Kenobi"), None, None, None, None, None, None, "string"),
            TypedTriple(sw1, "starring", Some(highestUid + 1), None, None, None, None, None, None, None, "uid")
          ).toSet

      assert(afterTriples !== beforeTriples)
      assert(afterTriples === expectedAfterTriples)
    }
  }
}
