/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import uk.co.gresearch.spark.dgraph.connector.model.GraphTableModel

/**
 * Partition of Dgraph data. Reads all triples with the given predicates in the given uid range.
 *
 * @param targets Dgraph alpha nodes
 * @param predicates optional predicates to read
 * @param uids optional uid ranges
 */
case class Partition(targets: Seq[Target], predicates: Option[Set[Predicate]], uids: Option[UidRange], model: GraphTableModel)
  extends InputPartition[InternalRow] {

  // TODO: use host names of Dgraph alphas to co-locate partitions
  override def preferredLocations(): Array[String] = super.preferredLocations()

  override def createPartitionReader(): InputPartitionReader[InternalRow] =
    new TriplePartitionReader(this, model)

  /**
   * Provide the query representing this partitions sub-graph.
   * @return partition query
   */
  def query: PartitionQuery = PartitionQuery.of(this)

}
