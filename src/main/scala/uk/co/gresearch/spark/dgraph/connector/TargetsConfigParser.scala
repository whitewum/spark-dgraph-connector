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

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.sources.v2.DataSourceOptions

trait TargetsConfigParser extends ConfigParser {

  protected def getTargets(options: DataSourceOptions): Seq[Target] = {
    val objectMapper = new ObjectMapper()
    val fromTargets = Seq(TargetsOption, "paths").flatMap(option =>
      getStringOption(option, options).map { pathStr =>
        objectMapper.readValue(pathStr, classOf[Array[String]]).toSeq
      }.getOrElse(Seq.empty[String])
    )

    val fromTarget = Seq(TargetOption, "path").flatMap(getStringOption(_, options))

    val allTargets = fromTargets ++ fromTarget
    if (allTargets.isEmpty)
      throw new IllegalArgumentException("No Dgraph servers provided, provide targets via " +
        "DataFrameReader.load(…) or DataFrameReader.option(TargetOption, …)"
      )

    allTargets.map(Target)
  }

}
