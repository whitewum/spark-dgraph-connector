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

import java.util.UUID

import io.dgraph.DgraphProto.TxnContext
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.connector.{ClusterState, Predicate, Schema, Target, Transaction}

class TestDefaultPartitionerOption extends FunSpec {

  describe("DefaultPartitionerOption") {
    val target = Target("localhost:9080")
    val schema = Schema(Set(Predicate("pred", "string")))
    val state = ClusterState(
      Map("1" -> Set(target)),
      Map("1" -> schema.predicates.map(_.predicateName)),
      10000,
      UUID.randomUUID()
    )
    val transaction = Transaction(TxnContext.newBuilder().build())
    val options = CaseInsensitiveStringMap.empty()

    it(s"should provide a partitioner") {
      new DefaultPartitionerOption().getPartitioner(schema, state, transaction, options)
    }
  }
}
