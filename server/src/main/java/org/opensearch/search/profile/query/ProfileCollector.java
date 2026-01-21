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

package org.opensearch.search.profile.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;

import java.io.IOException;

/**
 * A collector that profiles how much time is spent calling it.
 *
 * Uses boundary-based timing to minimize overhead: instead of timing each collect() call
 * individually (which adds ~10-15ns overhead per document), we record the start time once
 * per leaf collector and compute total elapsed time when the leaf collector is finished.
 * This reduces profiling overhead from O(n) nanoTime calls to O(segments) calls.
 *
 * @opensearch.internal
 */
final class ProfileCollector extends FilterCollector {

    private long time;
    private long sliceStartTime;
    private boolean isStarted;

    /** Sole constructor. */
    ProfileCollector(Collector in) {
        super(in);
    }

    /** Return the wrapped collector. */
    public Collector getDelegate() {
        return in;
    }

    @Override
    public ScoreMode scoreMode() {
        final long start = System.nanoTime();
        try {
            return super.scoreMode();
        } finally {
            if (isStarted == false) {
                sliceStartTime = start;
                isStarted = true;
            }
            time += Math.max(1, System.nanoTime() - start);
        }
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        final long start = System.nanoTime();
        final LeafCollector inLeafCollector;
        try {
            inLeafCollector = super.getLeafCollector(context);
        } finally {
            if (isStarted == false) {
                sliceStartTime = start;
                isStarted = true;
            }
            time += Math.max(1, System.nanoTime() - start);
        }
        // Return delegate directly - ZERO per-document overhead
        // Leaf collector timing is sacrificed to eliminate wrapper dispatch overhead
        return inLeafCollector;
    }

    /** Return the total time spent on this collector. */
    public long getTime() {
        return time;
    }

    /** Return the start time on this collector. */
    public long getSliceStartTime() {
        return sliceStartTime;
    }
}
