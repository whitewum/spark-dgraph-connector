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

package uk.co.gresearch.spark.dgraph.connector.partitioner

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector._

class DefaultPartitionerOption extends ConfigPartitionerOption {

  // for wide nodes we use uid range partitioner, for all other sources we use predicate + uid range
  val defaultPartitionerName = s"$PredicatePartitionerOption+$UidRangePartitionerOption"
  val wideNodeDefaultPartitionerName = s"$UidRangePartitionerOption"

  override def getPartitioner(schema: Schema,
                              clusterState: ClusterState,
                              options: CaseInsensitiveStringMap): Option[Partitioner] =
    if (getStringOption(NodesModeOption, options).contains(NodesModeWideOption))
      Some(getPartitioner(wideNodeDefaultPartitionerName, schema, clusterState, options))
    else
      Some(getPartitioner(defaultPartitionerName, schema, clusterState, options))

}
