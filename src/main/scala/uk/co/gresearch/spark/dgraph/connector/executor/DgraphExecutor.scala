package uk.co.gresearch.spark.dgraph.connector.executor

import io.grpc.ManagedChannel
import uk.co.gresearch.spark.dgraph.connector.{GraphQl, Json, Target, Transaction, getClientFromChannel, toChannel}

/**
 * A QueryExecutor implementation that executes a GraphQl query against a Dgraph cluster returning a Json result.
 * All queries are executed in the given transaction.
 *
 * @param transaction transaction
 * @param targets dgraph cluster targets
 */
case class DgraphExecutor(transaction: Transaction, targets: Seq[Target]) extends JsonGraphQlExecutor {

  /**
   * Executes a GraphQl query against a Dgraph cluster and returns the JSON query result.
   *
   * @param query: the query
   * @return a Json result
   */
  override def query(query: GraphQl): Json = {
    val channels: Seq[ManagedChannel] = targets.map(toChannel)
    try {
      val client = getClientFromChannel(channels)
      val response = client.newReadOnlyTransaction(transaction.context).query(query.string)
      Json(response.getJson.toStringUtf8)
    } finally {
      channels.foreach(_.shutdown())
    }
  }

}
