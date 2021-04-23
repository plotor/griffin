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

package org.apache.griffin.measure.step.builder

import org.apache.griffin.measure.configuration.dqdefinition.RuleParam
import org.apache.griffin.measure.context.DQContext
import org.apache.griffin.measure.step.DQStep
import org.apache.griffin.measure.step.transform.SparkSqlTransformStep

/**
 * spark-sql 类型 DSL 对应的 DQ 任务构造器实现
 */
case class SparkSqlDQStepBuilder() extends RuleParamStepBuilder {

  /**
   * 构造 spark-sql 类型的 DQ 任务步骤
   *
   * @param context
   * @param ruleParam
   * @return
   */
  def buildSteps(context: DQContext, ruleParam: RuleParam): Seq[DQStep] = {
    // 获取 step 名称，默认使用 out.dataframe.name 配置，否则按序生成一个
    val name = getStepName(ruleParam.getOutDfName())
    val transformStep = SparkSqlTransformStep(
      name,
      ruleParam.getRule,
      ruleParam.getDetails,
      None,
      ruleParam.getCache)
    //
    transformStep +: buildDirectWriteSteps(ruleParam)
  }

}
