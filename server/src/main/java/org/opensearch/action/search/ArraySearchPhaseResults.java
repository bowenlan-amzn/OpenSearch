/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.search;

import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.search.SearchPhaseResult;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * This class acts as a basic result collection that can be extended to do on-the-fly reduction or result processing
 *
 * @opensearch.internal
 */
class ArraySearchPhaseResults<Result extends SearchPhaseResult> extends SearchPhaseResults<Result> {
    final AtomicArray<Result> results;
    private final AtomicInteger streamCounters = new AtomicInteger(0);
    private static final int MAX_BATCHES_PER_SHARD = 100;

    ArraySearchPhaseResults(int size) {
        super(size);
        // Size the results array to accommodate multiple results per shard
        this.results = new AtomicArray<>(size * MAX_BATCHES_PER_SHARD);
    }

    Stream<Result> getSuccessfulResults() {
        return results.asList().stream();
    }

    @Override
    void consumeResult(Result result, Runnable next) {
        // Check if this is a streaming result that should get a batch ID
        if (result.getStreamBatchId() == 0) {
            final int batchId = streamCounters.incrementAndGet();
            result.setStreamBatchId(batchId);
            // Store the result at the batch id
            results.set(batchId, result);
        }

        next.run();
    }

    boolean hasResult(int shardIndex) {
        return results.get(shardIndex) != null;
    }

    @Override
    public AtomicArray<Result> getAtomicArray() {
        return results;
    }
}
