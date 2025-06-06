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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.SqlFactory;
import org.apache.flink.table.operations.utils.OperationExpressionsUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Relational operation that performs computations on top of subsets of input rows grouped by key.
 */
@Internal
public class AggregateQueryOperation implements QueryOperation {

    private static final String INPUT_ALIAS = "$$T_AGG";
    private final List<ResolvedExpression> groupingExpressions;
    private final List<ResolvedExpression> aggregateExpressions;
    private final QueryOperation child;
    private final ResolvedSchema resolvedSchema;

    public AggregateQueryOperation(
            List<ResolvedExpression> groupingExpressions,
            List<ResolvedExpression> aggregateExpressions,
            QueryOperation child,
            ResolvedSchema resolvedSchema) {
        this.groupingExpressions = groupingExpressions;
        this.aggregateExpressions = aggregateExpressions;
        this.child = child;
        this.resolvedSchema = resolvedSchema;
    }

    @Override
    public ResolvedSchema getResolvedSchema() {
        return resolvedSchema;
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("group", groupingExpressions);
        args.put("agg", aggregateExpressions);

        return OperationUtils.formatWithChildren(
                "Aggregate", args, getChildren(), Operation::asSummaryString);
    }

    public List<ResolvedExpression> getGroupingExpressions() {
        return groupingExpressions;
    }

    public List<ResolvedExpression> getAggregateExpressions() {
        return aggregateExpressions;
    }

    @Override
    public String asSerializableString(SqlFactory sqlFactory) {
        final String groupingExprs = getGroupingExprs(sqlFactory);
        return String.format(
                "SELECT %s FROM (%s\n) %s\nGROUP BY %s",
                Stream.concat(groupingExpressions.stream(), aggregateExpressions.stream())
                        .map(
                                expr ->
                                        OperationExpressionsUtils.scopeReferencesWithAlias(
                                                INPUT_ALIAS, expr))
                        .map(
                                resolvedExpression ->
                                        resolvedExpression.asSerializableString(sqlFactory))
                        .collect(Collectors.joining(", ")),
                OperationUtils.indent(child.asSerializableString(sqlFactory)),
                INPUT_ALIAS,
                groupingExprs);
    }

    private String getGroupingExprs(SqlFactory sqlFactory) {
        if (groupingExpressions.isEmpty()) {
            return "1";
        } else {
            return groupingExpressions.stream()
                    .map(
                            expr ->
                                    OperationExpressionsUtils.scopeReferencesWithAlias(
                                            INPUT_ALIAS, expr))
                    .map(resolvedExpression -> resolvedExpression.asSerializableString(sqlFactory))
                    .collect(Collectors.joining(", "));
        }
    }

    @Override
    public List<QueryOperation> getChildren() {
        return Collections.singletonList(child);
    }

    @Override
    public <T> T accept(QueryOperationVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
