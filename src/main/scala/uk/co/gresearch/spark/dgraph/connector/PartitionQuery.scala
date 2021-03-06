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

case class PartitionQuery(resultName: String, predicates: Option[Set[Predicate]], uids: Option[UidRange]) {

  def pagination: String =
    uids.map(range => s", first: ${range.length}, offset: ${range.first}").getOrElse("")

  val predicateQueries: Map[String, String] =
    predicates
      .getOrElse(Set.empty)
      .zipWithIndex
      .map { case (pred, idx) => s"pred${idx+1}" -> s"pred${idx+1} as var(func: has(<${pred.predicateName}>))" }
      .toMap

  val predicatePaths: Seq[String] =
    predicates
      .getOrElse(Set.empty)
      .map {
        case Predicate(predicate, "uid") => s"<$predicate> { uid }"
        case Predicate(predicate, _____) => s"<$predicate>"
      }
      .toSeq

  def forProperties: GraphQl = {
    val query =
      if (predicates.isEmpty) {
        s"""{
           |  ${resultName} (func: has(dgraph.type)$pagination) {
           |    uid
           |    dgraph.graphql.schema
           |    dgraph.type
           |    expand(_all_)
           |  }
           |}""".stripMargin
      } else {
        // this assumes all given predicates are all properties, no edges
        // TODO: make this else branch produce a query that returns only properties even if edges are in predicates
        //       https://github.com/G-Research/spark-dgraph-connector/issues/19
        forPropertiesAndEdges.string
      }

    GraphQl(query)
  }

  def forPropertiesAndEdges: GraphQl = {
    val query =
      if (predicates.isEmpty) {
        s"""{
           |  ${resultName} (func: has(dgraph.type)${pagination}) {
           |    uid
           |    dgraph.graphql.schema
           |    dgraph.type
           |    expand(_all_) {
           |      uid
           |    }
           |  }
           |}""".stripMargin
      } else {
        s"""{${predicateQueries.values.map(query => s"\n  $query").mkString}${if(predicateQueries.nonEmpty) "\n" else ""}
           |  ${resultName} (func: uid(${predicateQueries.keys.mkString(",")})${pagination}) {
           |    uid
           |${predicatePaths.map(path => s"    $path\n").mkString}  }
           |}""".stripMargin
      }

    GraphQl(query)
  }

  def countUids: GraphQl = {
    val query =
      if (predicates.isEmpty) {
        s"""{
           |  ${resultName} (func: has(dgraph.type)${pagination}) {
           |    count(uid)
           |  }
           |}""".stripMargin
      } else {
        s"""{${predicateQueries.values.map(query => s"\n  $query").mkString}${if(predicateQueries.nonEmpty) "\n" else ""}
           |  ${resultName} (func: uid(${predicateQueries.keys.mkString(",")})${pagination}) {
           |    count(uid)
           |  }
           |}""".stripMargin
      }
    GraphQl(query)
  }

}

object PartitionQuery {
  def of(partition: Partition, resultName: String = "result"): PartitionQuery =
    PartitionQuery(resultName, partition.predicates, partition.uids)
}
