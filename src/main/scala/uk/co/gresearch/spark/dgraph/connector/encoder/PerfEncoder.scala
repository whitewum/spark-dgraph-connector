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

package uk.co.gresearch.spark.dgraph.connector.encoder

import com.google.gson.Gson
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import uk.co.gresearch.spark.dgraph.connector.{Json, Perf, PerfJson}

/**
 * Encodes a perf json response.
 */
case class PerfEncoder() extends JsonNodeInternalRowEncoder {

  /**
   * Returns the schema of this table. If the table is not readable and doesn't have a schema, an
   * empty schema can be returned here.
   * From: org.apache.spark.sql.connector.catalog.Table.schema
   */
  override def schema(): StructType = Encoders.product[Perf].schema

  /**
   * Returns the actual schema of this data source scan, which may be different from the physical
   * schema of the underlying storage, as column pruning or other optimizations may happen.
   * From: org.apache.spark.sql.connector.read.Scan.readSchema
   */
  override def readSchema(): StructType = schema()

  /**
   * Encodes the given perf json result into InternalRows.
   *
   * @param json   perf json result
   * @param member member in the json that has the result
   * @return internal rows
   */
  override def fromJson(json: Json, member: String): Iterator[InternalRow] = {
    val perf = new Gson().fromJson(json.string, classOf[PerfJson])
    Iterator(InternalRow(
      new GenericArrayData(perf.partitionTargets.map(UTF8String.fromString)),
      Option(perf.partitionPredicates).map(p => new GenericArrayData(p.map(UTF8String.fromString))).orNull,
      perf.partitionUidsFirst,
      perf.partitionUidsLength,

      perf.sparkStageId,
      perf.sparkStageAttemptNumber,
      perf.sparkPartitionId,
      perf.sparkAttemptNumber,
      perf.sparkTaskAttemptId,

      perf.dgraphAssignTimestamp,
      perf.dgraphParsing,
      perf.dgraphProcessing,
      perf.dgraphEncoding,
      perf.dgraphTotal
    ))
  }

}
