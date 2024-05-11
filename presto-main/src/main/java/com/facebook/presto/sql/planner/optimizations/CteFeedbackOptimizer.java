/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.cost.CachingCostProvider;
import com.facebook.presto.cost.CachingStatsProvider;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.CostProvider;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;

import java.util.Optional;

public class CteFeedbackOptimizer
        implements PlanOptimizer
{
    private final Metadata metadata;

    private final StatsCalculator statsCalculator;

    private final CostComparator costComparator;

    private final CostCalculator costCalculator;

    public CteFeedbackOptimizer(Metadata metadata, StatsCalculator statsCalculator, CostComparator costComparator, CostCalculator costCalculator)
    {
        this.metadata = metadata;
        this.statsCalculator = statsCalculator;
        this.costComparator = costComparator;
        this.costCalculator = costCalculator;
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (session.orignalPlan.isPresent()) {
            PlanNode originalPlan = session.orignalPlan.get();
            // cte was materialized, perform feedback
            StatsProvider statsProvider = new CachingStatsProvider(
                    statsCalculator,
                    Optional.empty(),
                    Lookup.noLookup(),
                    session,
                    TypeProvider.viewOf(variableAllocator.getVariables()));
            CostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider, Optional.empty(), session);
            originalPlan =
                    new HistoricalStatisticsEquivalentPlanMarkingOptimizer(statsCalculator).optimize(originalPlan, session, types, variableAllocator, idAllocator, warningCollector).getPlanNode();

            return PlanOptimizerResult.optimizerResult(plan, false);
        }
        return PlanOptimizerResult.optimizerResult(plan, false);
    }
}