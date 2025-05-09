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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.runtime.partitioner.ForwardForUnspecifiedPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An edge in the streaming topology. One edge like this does not necessarily gets converted to a
 * connection between two job vertices (due to chaining/optimization).
 */
@Internal
public class StreamEdge implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final long ALWAYS_FLUSH_BUFFER_TIMEOUT = 0L;

    private final String edgeId;

    private final int sourceId;
    private final int targetId;

    /**
     * Note that this field doesn't have to be unique among all {@link StreamEdge}s. It's enough if
     * this field ensures that all logical instances of {@link StreamEdge} are unique, and {@link
     * #hashCode()} are different and {@link #equals(Object)} returns false, for every possible pair
     * of {@link StreamEdge}. Especially among two different {@link StreamEdge}s that are connecting
     * the same pair of nodes.
     */
    private final int uniqueId;

    /** The type number of the input for co-tasks. */
    private int typeNumber;

    /** The side-output tag (if any) of this {@link StreamEdge}. */
    private final OutputTag outputTag;

    /** The {@link StreamPartitioner} on this {@link StreamEdge}. */
    private StreamPartitioner<?> outputPartitioner;

    /** The name of the operator in the source vertex. */
    private final String sourceOperatorName;

    /** The name of the operator in the target vertex. */
    private final String targetOperatorName;

    private StreamExchangeMode exchangeMode;

    private long bufferTimeout;

    private boolean supportsUnalignedCheckpoints = true;

    private final IntermediateDataSetID intermediateDatasetIdToProduce;

    /**
     * There are relationships between multiple inputs, if the data corresponding to a specific join
     * key from one input is split, the corresponding join key data from the other inputs must be
     * duplicated (meaning that it must be sent to the downstream nodes where the split data is
     * sent).
     */
    private boolean interInputsKeysCorrelated;

    /**
     * For this edge the data corresponding to a specific join key must be sent to the same
     * downstream subtask.
     */
    private boolean intraInputKeyCorrelated;

    public StreamEdge(
            StreamNode sourceVertex,
            StreamNode targetVertex,
            int typeNumber,
            StreamPartitioner<?> outputPartitioner,
            OutputTag outputTag) {

        this(
                sourceVertex,
                targetVertex,
                typeNumber,
                ALWAYS_FLUSH_BUFFER_TIMEOUT,
                outputPartitioner,
                outputTag,
                StreamExchangeMode.UNDEFINED,
                0,
                null);
    }

    public StreamEdge(
            StreamNode sourceVertex,
            StreamNode targetVertex,
            int typeNumber,
            StreamPartitioner<?> outputPartitioner,
            OutputTag outputTag,
            StreamExchangeMode exchangeMode,
            int uniqueId,
            IntermediateDataSetID intermediateDatasetId) {

        this(
                sourceVertex,
                targetVertex,
                typeNumber,
                sourceVertex.getBufferTimeout(),
                outputPartitioner,
                outputTag,
                exchangeMode,
                uniqueId,
                intermediateDatasetId);
    }

    public StreamEdge(
            StreamNode sourceVertex,
            StreamNode targetVertex,
            int typeNumber,
            long bufferTimeout,
            StreamPartitioner<?> outputPartitioner,
            OutputTag outputTag,
            StreamExchangeMode exchangeMode,
            int uniqueId,
            IntermediateDataSetID intermediateDatasetId) {

        this.sourceId = sourceVertex.getId();
        this.targetId = targetVertex.getId();
        this.uniqueId = uniqueId;
        this.typeNumber = typeNumber;
        this.bufferTimeout = bufferTimeout;
        this.outputPartitioner = outputPartitioner;
        this.outputTag = outputTag;
        this.sourceOperatorName = sourceVertex.getOperatorName();
        this.targetOperatorName = targetVertex.getOperatorName();
        this.exchangeMode = checkNotNull(exchangeMode);
        this.intermediateDatasetIdToProduce = intermediateDatasetId;
        this.edgeId =
                sourceVertex
                        + "_"
                        + targetVertex
                        + "_"
                        + typeNumber
                        + "_"
                        + outputPartitioner
                        + "_"
                        + uniqueId;
        if (outputPartitioner != null) {
            configureKeyCorrelation(outputPartitioner);
        }
    }

    public int getSourceId() {
        return sourceId;
    }

    public int getTargetId() {
        return targetId;
    }

    public int getTypeNumber() {
        return typeNumber;
    }

    public OutputTag getOutputTag() {
        return this.outputTag;
    }

    public StreamPartitioner<?> getPartitioner() {
        return outputPartitioner;
    }

    public StreamExchangeMode getExchangeMode() {
        return exchangeMode;
    }

    void setExchangeMode(StreamExchangeMode exchangeMode) {
        this.exchangeMode = exchangeMode;
    }

    public void setPartitioner(StreamPartitioner<?> partitioner) {
        configureKeyCorrelation(partitioner);
        this.outputPartitioner = partitioner;
    }

    public void setBufferTimeout(long bufferTimeout) {
        checkArgument(bufferTimeout >= -1);
        this.bufferTimeout = bufferTimeout;
    }

    public long getBufferTimeout() {
        return bufferTimeout;
    }

    public void setSupportsUnalignedCheckpoints(boolean supportsUnalignedCheckpoints) {
        this.supportsUnalignedCheckpoints = supportsUnalignedCheckpoints;
    }

    public void setTypeNumber(int typeNumber) {
        this.typeNumber = typeNumber;
    }

    public boolean supportsUnalignedCheckpoints() {
        return supportsUnalignedCheckpoints;
    }

    @Override
    public int hashCode() {
        return Objects.hash(edgeId, outputTag);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StreamEdge that = (StreamEdge) o;
        return Objects.equals(edgeId, that.edgeId) && Objects.equals(outputTag, that.outputTag);
    }

    @Override
    public String toString() {
        return "("
                + (sourceOperatorName + "-" + sourceId)
                + " -> "
                + (targetOperatorName + "-" + targetId)
                + ", typeNumber="
                + typeNumber
                + ", outputPartitioner="
                + outputPartitioner
                + ", exchangeMode="
                + exchangeMode
                + ", bufferTimeout="
                + bufferTimeout
                + ", outputTag="
                + outputTag
                + ", uniqueId="
                + uniqueId
                + ')';
    }

    public IntermediateDataSetID getIntermediateDatasetIdToProduce() {
        return intermediateDatasetIdToProduce;
    }

    public String getEdgeId() {
        return edgeId;
    }

    private void configureKeyCorrelation(StreamPartitioner<?> partitioner) {
        // Set a safe value of correlations based on the partitioner to ensure the program can
        // work normally by default. The final value of the correlations can be flexibly determined
        // by the operator.
        if (partitioner.isPointwise()) {
            this.intraInputKeyCorrelated =
                    partitioner instanceof ForwardPartitioner
                            && !(partitioner instanceof ForwardForUnspecifiedPartitioner);
            this.interInputsKeysCorrelated = false;
        } else {
            this.intraInputKeyCorrelated = !(partitioner instanceof RebalancePartitioner);
            this.interInputsKeysCorrelated = !(partitioner instanceof RebalancePartitioner);
        }
    }

    public boolean areInterInputsKeysCorrelated() {
        return interInputsKeysCorrelated;
    }

    public boolean isIntraInputKeyCorrelated() {
        return intraInputKeyCorrelated;
    }

    public void setIntraInputKeyCorrelated(boolean intraInputKeyCorrelated) {
        this.intraInputKeyCorrelated = intraInputKeyCorrelated;
    }
}
