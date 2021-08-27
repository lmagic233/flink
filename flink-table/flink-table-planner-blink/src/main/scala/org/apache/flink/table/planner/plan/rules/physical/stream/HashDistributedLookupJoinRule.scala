/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.rel.RelCollations
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.planner.calcite.FlinkContext
import org.apache.flink.table.planner.plan.`trait`.{FlinkRelDistribution, ModifyKindSetTraitDef, UpdateKindTraitDef}
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalExchange, StreamPhysicalLookupJoin}
import org.apache.flink.table.planner.plan.rules.physical.FlinkExpandConversionRule

import java.util

class HashDistributedLookupJoinRule extends RelOptRule(
  operand(classOf[StreamPhysicalLookupJoin], any()),
  "HashDistributedLookupJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val tableConfig = call.getPlanner.getContext.unwrap(classOf[FlinkContext]).getTableConfig
    tableConfig.getConfiguration.getBoolean(ExecutionConfigOptions.TABLE_EXEC_LOOKUP_DISTRIBUTE_BY_KEY)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val originalLookupJoin: StreamPhysicalLookupJoin = call.rel(0)
    val joinInfo = originalLookupJoin.joinInfo

    val requiredDistribution = FlinkRelDistribution.hash(joinInfo.leftKeys)

    val hashDistributedInput = FlinkExpandConversionRule.satisfyDistribution(
      FlinkConventions.STREAM_PHYSICAL,
      originalLookupJoin.getInput,
      requiredDistribution
    )

    call.transformTo(
      originalLookupJoin.copy(originalLookupJoin.getTraitSet, util.Arrays.asList(hashDistributedInput))
    )
  }
}

object HashDistributedLookupJoinRule {
  val INSTANCE: RelOptRule = new HashDistributedLookupJoinRule
}
