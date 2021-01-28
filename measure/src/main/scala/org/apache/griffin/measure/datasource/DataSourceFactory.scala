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

package org.apache.griffin.measure.datasource

import scala.util.Success

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

import org.apache.griffin.measure.Loggable
import org.apache.griffin.measure.configuration.dqdefinition.DataSourceParam
import org.apache.griffin.measure.datasource.cache.StreamingCacheClientFactory
import org.apache.griffin.measure.datasource.connector.DataConnectorFactory

object DataSourceFactory extends Loggable {

  /**
   * 基于配置构造并返回对应的数据源实例集合
   * @param sparkSession
   * @param ssc
   * @param dataSources
   * @return
   */
  def getDataSources(
      sparkSession: SparkSession,
      ssc: StreamingContext,
      dataSources: Seq[DataSourceParam]): Seq[DataSource] = {
    // 基于 DataSource 参数构造 DataSource 实例
    dataSources.zipWithIndex.flatMap {
      case (param, index) => getDataSource(sparkSession, ssc, param, index)
    }
  }

  private def getDataSource(
      sparkSession: SparkSession,
      ssc: StreamingContext,
      dataSourceParam: DataSourceParam,
      index: Int): Option[DataSource] = {
    val name = dataSourceParam.getName
    val timestampStorage = TimestampStorage()

    // for streaming data cache
    val streamingCacheClientOpt = StreamingCacheClientFactory.getClientOpt(
      sparkSession,
      dataSourceParam.getCheckpointOpt,
      name,
      index,
      timestampStorage)

    val connectorParamsOpt = dataSourceParam.getConnector

    connectorParamsOpt match {
      case Some(connectorParam) =>
        // 获取对应的 Connector 实例
        val dataConnectors = DataConnectorFactory.getDataConnector(
          sparkSession,
          ssc,
          connectorParam,
          timestampStorage,
          streamingCacheClientOpt) match {
          case Success(connector) => Some(connector)
          case _ => None
        }

        Some(DataSource(name, dataSourceParam, dataConnectors, streamingCacheClientOpt))
      case None => None
    }
  }

}
