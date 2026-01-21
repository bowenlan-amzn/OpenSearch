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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.search.profile.aggregation;

import org.apache.lucene.search.Scorable;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.profile.Timer;

import java.io.IOException;

/**
 * The collector for the agg profiles.
 *
 * Uses boundary-based timing with ZERO per-document overhead. Timing starts when the
 * collector is created and ends when finish() is called. The collect() method is pure
 * delegation with no additional operations.
 *
 * @opensearch.internal
 */
public class ProfilingLeafBucketCollector extends LeafBucketCollector {

    private final LeafBucketCollector delegate;
    private final Timer collectTimer;
    private final long startTimeNanos;

    public ProfilingLeafBucketCollector(LeafBucketCollector delegate, AggregationProfileBreakdown profileBreakdown) {
        this.delegate = delegate;
        this.collectTimer = profileBreakdown.getTimer(AggregationTimingType.COLLECT);
        this.startTimeNanos = System.nanoTime();  // Start timing at construction
    }

    @Override
    public void collect(int doc, long bucket) throws IOException {
        delegate.collect(doc, bucket);  // ZERO overhead - pure delegation
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        delegate.setScorer(scorer);
    }

    /**
     * Finish timing for this collector and record the elapsed time.
     * Called when collection for this segment is complete.
     */
    public void finish() {
        long elapsedNanos = System.nanoTime() - startTimeNanos;
        collectTimer.addExternalTiming(elapsedNanos, 1);
    }
}
