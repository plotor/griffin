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

package org.apache.griffin.measure.launch.batch

import java.util.concurrent.TimeUnit

import scala.util.Try

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.griffin.measure.configuration.dqdefinition._
import org.apache.griffin.measure.configuration.enums.ProcessType.BatchProcessType
import org.apache.griffin.measure.context._
import org.apache.griffin.measure.datasource.DataSourceFactory
import org.apache.griffin.measure.job.builder.DQJobBuilder
import org.apache.griffin.measure.launch.DQApp
import org.apache.griffin.measure.step.builder.udf.GriffinUDFAgent
import org.apache.griffin.measure.utils.CommonUtils

case class BatchDQApp(allParam: GriffinConfig) extends DQApp {

  val envParam: EnvConfig = allParam.getEnvConfig
  val dqParam: DQConfig = allParam.getDqConfig

  val sparkParam: SparkParam = envParam.getSparkParam
  // DQ Job 名称
  val metricName: String = dqParam.getName
  val sinkParams: Seq[SinkParam] = getSinkParams

  var dqContext: DQContext = _

  def retryable: Boolean = false

  def init: Try[_] = Try {
    // build spark 2.0+ application context
    val conf = new SparkConf().setAppName(metricName)
    conf.setAll(sparkParam.getConfig) // 设置用户自定义 spark 参数
    // 允许对两个 DataFrame 执行 join 操作
    conf.set("spark.sql.crossJoin.enabled", "true")
    sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val logLevel = getGriffinLogLevel
    sparkSession.sparkContext.setLogLevel(sparkParam.getLogLevel)
    griffinLogger.setLevel(logLevel)

    // register udf
    GriffinUDFAgent.register(sparkSession)
  }

  def run: Try[Boolean] = {
    // 执行 {...} 逻辑，并打印执行开销
    val result = CommonUtils.timeThis({
      // measure 的时间
      val measureTime = getMeasureTime
      // 构造 DQ 任务上下文 ID，默认就是 measure 时间戳
      val contextId = ContextId(measureTime)

      // 基于数据源配置，构造并返回对应的 DataSource 实例集合
      val dataSources =
        DataSourceFactory.getDataSources(sparkSession, null, dqParam.getDataSources)
      dataSources.foreach(_.init()) // 目前仅 Streaming 监控任务有初始化逻辑

      // 创建 DQ Batch 任务上下文对象
      dqContext =
        DQContext(contextId, metricName, dataSources, sinkParams, BatchProcessType)(sparkSession)

      // start id
      val applicationId = sparkSession.sparkContext.applicationId
      // 调用 Sink#open 方法初始化到对应 Sink 系统的连接（按需）
      dqContext.getSinks.foreach(_.open(applicationId))

      // 基于任务上下文和规则配置构造 DQ Batch 任务
      val dqJob = DQJobBuilder.buildDQJob(dqContext, dqParam.getEvaluateRule)

      // 执行 DQ 任务
      dqJob.execute(dqContext)
    }, TimeUnit.MILLISECONDS)

    // clean context
    dqContext.clean()

    // finish
    dqContext.getSinks.foreach(_.close())

    result
  }

  def close: Try[_] = Try {
    sparkSession.stop()
  }

}
