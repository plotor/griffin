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

package org.apache.griffin.measure

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import org.apache.griffin.measure.configuration.dqdefinition.{
  DQConfig,
  EnvConfig,
  GriffinConfig,
  Param
}
import org.apache.griffin.measure.configuration.dqdefinition.reader.ParamReaderFactory
import org.apache.griffin.measure.configuration.enums.ProcessType
import org.apache.griffin.measure.configuration.enums.ProcessType._
import org.apache.griffin.measure.launch.DQApp
import org.apache.griffin.measure.launch.batch.BatchDQApp
import org.apache.griffin.measure.launch.streaming.StreamingDQApp

/**
 * application entrance
 */
object Application extends Loggable {

  def main(args: Array[String]): Unit = {
    info(args.mkString("[", ", ", "]"))
    if (args.length < 2) {
      error("Usage: class <env-param> <dq-param>")
      sys.exit(-1)
    }

    // env 配置文件
    val envParamFile = args(0)
    // dq 配置文件
    val dqParamFile = args(1)

    info(envParamFile)
    info(dqParamFile)

    // 加载 env 相关配置
    val envParam = readParamFile[EnvConfig](envParamFile) match {
      case Success(p) => p
      case Failure(ex) =>
        error(ex.getMessage, ex)
        sys.exit(-2)
    }

    // 加载 dq 相关配置
    val dqParam = readParamFile[DQConfig](dqParamFile) match {
      case Success(p) => p
      case Failure(ex) =>
        error(ex.getMessage, ex)
        sys.exit(-2)
    }

    // 聚合 env 和 dq 配置
    val allParam: GriffinConfig = GriffinConfig(envParam, dqParam)

    // 设置运行模式：batch or streaming
    val procType = ProcessType.withNameWithDefault(allParam.getDqConfig.getProcType)
    val dqApp: DQApp = procType match {
      // 构建 Batch 类型任务
      case BatchProcessType => BatchDQApp(allParam)
      // 构建 Streaming 类型任务
      case StreamingProcessType => StreamingDQApp(allParam)
      case _ =>
        error(s"$procType is unsupported process type!")
        sys.exit(-4)
    }

    // 模板方法
    startup()

    // 初始化 SparkContext
    dqApp.init match {
      case Success(_) =>
        info("process init success")
      case Failure(ex) =>
        error(s"process init error: ${ex.getMessage}", ex)
        shutdown()
        sys.exit(-5)
    }

    // 执行 DQ Spark 任务
    val success = dqApp.run match {
      case Success(result) =>
        info("process run result: " + (if (result) "success" else "failed"))
        result

      case Failure(ex) =>
        error(s"process run error: ${ex.getMessage}", ex)

        if (dqApp.retryable) {
          // 抛出异常，触发 spark 重试执行
          throw ex
        } else {
          shutdown()
          sys.exit(-5)
        }
    }

    // 关闭 DQ App，目前都是关闭 Spark 会话
    dqApp.close match {
      case Success(_) =>
        info("process end success")
      case Failure(ex) =>
        error(s"process end error: ${ex.getMessage}", ex)
        shutdown()
        sys.exit(-5)
    }

    // 模板方法
    shutdown()

    if (!success) {
      sys.exit(-5)
    }
  }

  def readParamFile[T <: Param](file: String)(implicit m: ClassTag[T]): Try[T] = {
    val paramReader = ParamReaderFactory.getParamReader(file)
    // 基于 ParamReader 读取配置
    paramReader.readConfig[T]
  }

  private def startup(): Unit = {}

  private def shutdown(): Unit = {}

}
