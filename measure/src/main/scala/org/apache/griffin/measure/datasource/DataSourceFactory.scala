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
   * 基于数据源配置，构造并返回对应的 DataSource 实例集合
   *
   * @param sparkSession
   * @param ssc
   * @param dataSources
   * @return
   */
  def getDataSources(
      sparkSession: SparkSession,
      ssc: StreamingContext,
      dataSources: Seq[DataSourceParam]): Seq[DataSource] = { // 任务的数据源配置列表
    // 遍历 DataSource 参数配置列表，基于参数配置构造对应的 DataSource 实例
    dataSources.zipWithIndex.flatMap {
      // (DataSourceParam, index)
      case (param, index) =>
        // 解析 Data Source 配置，基于类型构造对应的 Connector 实例，封装成 DataSource 实例返回
        getDataSource(sparkSession, ssc, param, index)
    }
  }

  /**
   * 解析 Data Source 配置，基于类型构造对应的 Connector 实例，封装成 DataSource 实例返回
   *
   * @param sparkSession
   * @param ssc
   * @param dataSourceParam
   * @param index
   * @return
   */
  private def getDataSource(
      sparkSession: SparkSession,
      ssc: StreamingContext,
      dataSourceParam: DataSourceParam,
      index: Int): Option[DataSource] = { // index 参数用于 streaming
    // 获取数据源名称
    val name = dataSourceParam.getName
    // 创建 ts 管理实例
    val timestampStorage = TimestampStorage()

    // for streaming data cache
    val streamingCacheClientOpt = StreamingCacheClientFactory.getClientOpt(
      sparkSession,
      dataSourceParam.getCheckpointOpt,
      name,
      index,
      timestampStorage)

    // 获取数据源 Connector 参数配置
    val connectorParamsOpt = dataSourceParam.getConnector

    connectorParamsOpt match {
      case Some(connectorParam) =>
        // 基于参数配置构造对应的数据源 Connector 实例
        val dataConnector = DataConnectorFactory.getDataConnector(
          sparkSession,
          ssc,
          connectorParam,
          timestampStorage,
          streamingCacheClientOpt) match {
          case Success(connector) => Some(connector)
          case _ => None
        }

        Some(DataSource(name, dataSourceParam, dataConnector, streamingCacheClientOpt))
      case None => None
    }
  }

}
