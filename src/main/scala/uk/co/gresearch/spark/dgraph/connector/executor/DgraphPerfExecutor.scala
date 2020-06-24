package uk.co.gresearch.spark.dgraph.connector.executor
import com.google.gson.Gson
import io.dgraph.DgraphClient
import io.dgraph.DgraphProto.Response
import io.grpc.ManagedChannel
import org.apache.spark.TaskContext
import uk.co.gresearch.spark.dgraph.connector.{GraphQl, Json, Partition, PerfJson, getClientFromChannel, toChannel}

case class DgraphPerfExecutor(partition: Partition) extends JsonGraphQlExecutor {

  /**
   * Executes the given graphql query and returns the query result as json.
   *
   * @param query query
   * @return result
   */
  override def query(query: GraphQl): Json = {
    val channels: Seq[ManagedChannel] = partition.targets.map(toChannel)
    try {
      val client: DgraphClient = getClientFromChannel(channels)
      val response: Response = client.newReadOnlyTransaction().query(query.string)
      toJson(response)
    } finally {
      channels.foreach(_.shutdown())
    }
  }

  def toJson(response: Response): Json = {
    val task = TaskContext.get()
    val latency = Some(response).filter(_.hasLatency).map(_.getLatency)
    val perf =
      new PerfJson(
        partition.targets.map(_.target).toArray,
        partition.predicates.map(_.map(_.predicateName).toArray).orNull,
        partition.uids.map(_.first.asInstanceOf[java.lang.Long]).orNull,
        partition.uids.map(_.length.asInstanceOf[java.lang.Long]).orNull,

        task.stageId(),
        task.stageAttemptNumber(),
        task.partitionId(),
        task.attemptNumber(),
        task.taskAttemptId(),

        latency.map(_.getAssignTimestampNs.asInstanceOf[java.lang.Long]).orNull,
        latency.map(_.getParsingNs.asInstanceOf[java.lang.Long]).orNull,
        latency.map(_.getProcessingNs.asInstanceOf[java.lang.Long]).orNull,
        latency.map(_.getEncodingNs.asInstanceOf[java.lang.Long]).orNull,
        latency.map(_.getTotalNs.asInstanceOf[java.lang.Long]).orNull
      )
    val json = new Gson().toJson(perf)
    Json(json)
  }

}
