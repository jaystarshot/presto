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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.cost.CachingCostProvider;
import com.facebook.presto.cost.CachingStatsProvider;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.CostProvider;
import com.facebook.presto.cost.PlanCostEstimate;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.eventlistener.CTEInformation;
import com.facebook.presto.spi.plan.CteConsumerNode;
import com.facebook.presto.spi.plan.CteProducerNode;
import com.facebook.presto.spi.plan.CteReferenceNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.SequenceNode;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Ordering;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import com.google.common.graph.Traverser;
import io.airlift.units.Duration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;

import static com.facebook.presto.SystemSessionProperties.getCteMaterializationStrategy;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.CteMaterializationStrategy.ALL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/*
 * Transformation of CTE Reference Nodes:
 * This process converts CTE reference nodes into corresponding CteProducers and Consumers.
 * Makes sure that execution deadlocks do not exist
 *
 * Example:
 * Before Transformation:
 *   JOIN
 *   |-- CTEReference(cte2)
 *   |   `-- TABLESCAN2
 *   `-- CTEReference(cte3)
 *       `-- TABLESCAN3
 *
 * After Transformation:
 *   SEQUENCE(cte1)
 *   |-- CTEProducer(cte2)
 *   |   `-- TABLESCAN2
 *   |-- CTEProducer(cte3)
 *   |   `-- TABLESCAN3
 *   `-- JOIN
 *       |-- CTEConsumer(cte2)
 *       `-- CTEConsumer(cte3)
 */
public class LogicalCteOptimizer
        implements PlanOptimizer
{
    private final Metadata metadata;

    private final CostComparator costComparator;

    private final StatsCalculator statsCalculator;

    private final CostCalculator costCalculator;

    public LogicalCteOptimizer(Metadata metadata,
            CostComparator costComparator,
            CostCalculator costCalculator,
            StatsCalculator statsCalculator)
    {
        this.metadata = metadata;
        this.costComparator = costComparator;
        this.costCalculator = costCalculator;
        this.statsCalculator = statsCalculator;
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (!getCteMaterializationStrategy(session).equals(ALL)
                || session.getCteInformationCollector().getCTEInformationList().stream().noneMatch(CTEInformation::isMaterialized)) {
            return PlanOptimizerResult.optimizerResult(plan, false);
        }
        PlanNode rewrittenPlan = plan;
        Duration timeout = SystemSessionProperties.getOptimizerTimeout(session);
        StatsProvider statsProvider = new CachingStatsProvider(
                statsCalculator,
                Optional.empty(),
                Lookup.noLookup(),
                session,
                TypeProvider.viewOf(variableAllocator.getVariables()));
        CostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider, Optional.empty(), session);
        if (isMaterializeAllCtes(session)) {
            rewrittenPlan = new CteEnumerator(session, idAllocator, variableAllocator, costProvider).persistAllCtes(plan);
        }
        else {
            rewrittenPlan = new CteEnumerator(session, idAllocator, variableAllocator, costProvider).choosePersistentCtes(plan);
        }

        return PlanOptimizerResult.optimizerResult(rewrittenPlan, !rewrittenPlan.equals(plan));
    }

    public class CteEnumerator
    {
        private final Session session;
        private final PlanNodeIdAllocator planNodeIdAllocator;
        private final VariableAllocator variableAllocator;

        private final CostProvider costProvider;

        public CteEnumerator(Session session,
                PlanNodeIdAllocator planNodeIdAllocator,
                VariableAllocator variableAllocator,
                CostProvider costProvider)
        {
            this.session = session;
            this.planNodeIdAllocator = planNodeIdAllocator;
            this.variableAllocator = variableAllocator;
            this.costProvider = costProvider;
        }

        public PlanNode persistAllCtes(PlanNode root)
        {
            checkArgument(root.getSources().size() == 1, "expected newChildren to contain 1 node");
            CteTransformerContext context = new CteTransformerContext(Optional.empty());
            PlanNode transformedCte = SimplePlanRewriter.rewriteWith(new CteConsumerTransformer(planNodeIdAllocator, variableAllocator, CteConsumerTransformer.Operation.REWRITE),
                    root, context);
            List<PlanNode> topologicalOrderedList = context.getTopologicalOrdering();
            if (topologicalOrderedList.isEmpty()) {
                return root;
            }
            SequenceNode sequenceNode = new SequenceNode(root.getSourceLocation(), planNodeIdAllocator.getNextId(), topologicalOrderedList,
                    transformedCte.getSources().get(0));
            return root.replaceChildren(Arrays.asList(sequenceNode));
        }

        public PlanNode choosePersistentCtes(PlanNode root)
        {
            // cost based
            // ToDo cleanup
            CteTransformerContext context = new CteTransformerContext(Optional.empty());
            SimplePlanRewriter.rewriteWith(new CteConsumerTransformer(planNodeIdAllocator, variableAllocator, CteConsumerTransformer.Operation.EXPLORE),
                    root, context);
            List<PlanNode> cteProducerList = context.getTopologicalOrdering();
            if (session.getCteInformationCollector().getCTEInformationList().size() > 20) {
                // 2^n combinations which will be processed
                return root;
            }
            int numberOfCtes = cteProducerList.size();
            int combinations = 1 << numberOfCtes; // 2^n combinations

            List<CteEnumerationResult> candidates = new ArrayList<>();
            for (int i = 0; i < combinations; i++) {
                // For each combination, decide which CTEs to materialize
                List<PlanNode> materializedCtes = new ArrayList<>();
                for (int j = 0; j < numberOfCtes; j++) {
                    if ((i & (1 << j)) != 0) {
                        materializedCtes.add(cteProducerList.get(j));
                    }
                }
                // Generate the plan for this combination
                candidates.add(generatePlanForCombination(root, materializedCtes));
            }
            Ordering<CteEnumerationResult> resultComparator = costComparator.forSession(session).onResultOf(result -> result.cost);
            return resultComparator.min(candidates).getPlanNode().orElse(root);
        }

        public CteEnumerationResult generatePlanForCombination(PlanNode root, List<PlanNode> cteProducers)
        {
            CteTransformerContext context = new CteTransformerContext(Optional.of(cteProducers.stream()
                    .map(node -> ((CteProducerNode) node).getCteName()).collect(toImmutableSet())));
            PlanNode transformedCte =
                    SimplePlanRewriter.rewriteWith(new CteConsumerTransformer(planNodeIdAllocator, variableAllocator, CteConsumerTransformer.Operation.REWRITE),
                            root, context);
            PlanNode sequencePlan = new SequenceNode(root.getSourceLocation(), planNodeIdAllocator.getNextId(), cteProducers,
                    transformedCte.getSources().get(0));
            return CteEnumerationResult.createCteEnumeration(Optional.of(sequencePlan)
                    , costProvider.getCost(sequencePlan));
        }
    }

    public static class CteConsumerTransformer
            extends SimplePlanRewriter<CteTransformerContext>
    {
        private final PlanNodeIdAllocator idAllocator;

        private final VariableAllocator variableAllocator;

        enum Operation
        {
            EXPLORE,
            REWRITE,
        }

        private final Operation operation;

        public CteConsumerTransformer(PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator, Operation operation)
        {
            this.idAllocator = idAllocator;
            this.variableAllocator = variableAllocator;
            this.operation = operation;
        }

        @Override
        public PlanNode visitCteReference(CteReferenceNode node, RewriteContext<CteTransformerContext> context)
        {
            context.get().addDependency(node.getCteName());
            context.get().pushActiveCte(node.getCteName());
            // So that dependent CTEs are processed properly
            PlanNode actualSource = context.rewrite(node.getSource(), context.get());
            context.get().popActiveCte();
            CteProducerNode cteProducerSource = new CteProducerNode(node.getSourceLocation(),
                    idAllocator.getNextId(),
                    actualSource,
                    node.getCteName(),
                    variableAllocator.newVariable("rows", BIGINT), node.getOutputVariables());
            context.get().addProducer(node.getCteName(), cteProducerSource);
            if (operation.equals(Operation.EXPLORE) || !context.get().shouldMaterialize(node.getCteName())) {
                return node;
            }
            return new CteConsumerNode(node.getSourceLocation(), idAllocator.getNextId(), actualSource.getOutputVariables(), node.getCteName());
        }

        @Override
        public PlanNode visitApply(ApplyNode node, RewriteContext<CteTransformerContext> context)
        {
            return new ApplyNode(node.getSourceLocation(),
                    idAllocator.getNextId(),
                    context.rewrite(node.getInput(),
                            context.get()),
                    context.rewrite(node.getSubquery(),
                            context.get()),
                    node.getSubqueryAssignments(),
                    node.getCorrelation(),
                    node.getOriginSubqueryError(),
                    node.getMayParticipateInAntiJoin());
        }}

    public static class CteTransformerContext
    {
        public Map<String, CteProducerNode> cteProducerMap;

        // a -> b indicates that b needs to be processed before a
        MutableGraph<String> graph;
        public Stack<String> activeCteStack;

        public final Optional<Set<String>> ctesToMaterialize;

        public CteTransformerContext(Optional<Set<String>> ctesToMaterialize)
        {
            this.ctesToMaterialize = ctesToMaterialize;
            cteProducerMap = new HashMap<>();
            // The cte graph will never have cycles because sql won't allow it
            graph = GraphBuilder.directed().build();
            activeCteStack = new Stack<>();
        }

        public Map<String, CteProducerNode> getCteProducerMap()
        {
            return cteProducerMap;
        }

        public boolean shouldMaterialize(String cteName)
        {
            return ctesToMaterialize.map(strings -> strings.contains(cteName)).orElse(true);
        }

        public void addProducer(String cteName, CteProducerNode cteProducer)
        {
            cteProducerMap.putIfAbsent(cteName, cteProducer);
        }

        public void pushActiveCte(String cte)
        {
            this.activeCteStack.push(cte);
        }

        public String popActiveCte()
        {
            return this.activeCteStack.pop();
        }

        public Optional<String> peekActiveCte()
        {
            return (this.activeCteStack.isEmpty()) ? Optional.empty() : Optional.ofNullable(this.activeCteStack.peek());
        }

        public void addDependency(String currentCte)
        {
            graph.addNode(currentCte);
            Optional<String> parentCte = peekActiveCte();
            parentCte.ifPresent(s -> graph.putEdge(currentCte, s));
        }

        public List<PlanNode> getTopologicalOrdering()
        {
            List<PlanNode> topSortedCteProducerList = new ArrayList<>();
            Traverser.forGraph(graph).depthFirstPostOrder(graph.nodes())
                    .forEach(cteName -> topSortedCteProducerList.add(cteProducerMap.get(cteName)));
            return topSortedCteProducerList;
        }
    }

    @VisibleForTesting
    static class CteEnumerationResult
    {
        public static final CteEnumerationResult UNKNOWN_COST_RESULT = new CteEnumerationResult(Optional.empty(), PlanCostEstimate.unknown());
        public static final CteEnumerationResult INFINITE_COST_RESULT = new CteEnumerationResult(Optional.empty(), PlanCostEstimate.infinite());

        private final Optional<PlanNode> planNode;
        private final PlanCostEstimate cost;

        private CteEnumerationResult(Optional<PlanNode> planNode, PlanCostEstimate cost)
        {
            this.planNode = requireNonNull(planNode, "planNode is null");
            this.cost = requireNonNull(cost, "cost is null");
            checkArgument((cost.hasUnknownComponents() || cost.equals(PlanCostEstimate.infinite())) && !planNode.isPresent()
                            || (!cost.hasUnknownComponents() || !cost.equals(PlanCostEstimate.infinite())) && planNode.isPresent(),
                    "planNode should be present if and only if cost is known");
        }

        public Optional<PlanNode> getPlanNode()
        {
            return planNode;
        }

        public PlanCostEstimate getCost()
        {
            return cost;
        }

        static CteEnumerationResult createCteEnumeration(Optional<PlanNode> planNode, PlanCostEstimate cost)
        {
            if (cost.hasUnknownComponents()) {
                return UNKNOWN_COST_RESULT;
            }
            if (cost.equals(PlanCostEstimate.infinite())) {
                return INFINITE_COST_RESULT;
            }
            return new CteEnumerationResult(planNode, cost);
        }
    }
}
