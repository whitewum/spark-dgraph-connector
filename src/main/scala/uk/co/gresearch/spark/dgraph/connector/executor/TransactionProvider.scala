package uk.co.gresearch.spark.dgraph.connector.executor

import io.grpc.ManagedChannel
import uk.co.gresearch.spark.dgraph.connector.{Target, Transaction, getClientFromChannel, toChannel}

trait TransactionProvider {
  def getTransaction(targets: Seq[Target]): Transaction = {
    val channels: Seq[ManagedChannel] = targets.map(toChannel)
    try {
      val client = getClientFromChannel(channels)
      val transaction = client.newReadOnlyTransaction()
      val response = transaction.query("{ result (func: uid(0x0)) { } }")
      Transaction(response.getTxn)
    } finally {
      channels.foreach(_.shutdown())
    }
  }
}
