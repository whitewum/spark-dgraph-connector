package uk.co.gresearch.spark.dgraph.connector.partitioner

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector.executor.DgraphExecutor
import uk.co.gresearch.spark.dgraph.connector.{ClusterState, ConfigParser, MaxLeaseIdEstimatorOption, Transaction, UidCountEstimatorOption}

trait EstimatorProviderOption extends ConfigParser with ClusterStateHelper {

  def getEstimatorOption(option: String, options: CaseInsensitiveStringMap, default: String,
                         clusterState: ClusterState,
                         transaction: Transaction): UidCardinalityEstimator = {
    val name = getStringOption(option, options, default)
    name match {
      case MaxLeaseIdEstimatorOption => UidCardinalityEstimator.forMaxLeaseId(clusterState.maxLeaseId)
      case UidCountEstimatorOption => UidCardinalityEstimator.forExecutor(DgraphExecutor(transaction, getAllClusterTargets(clusterState)))
      case _ => throw new IllegalArgumentException(s"Unknown uid cardinality estimator: $name")
    }
  }

}
