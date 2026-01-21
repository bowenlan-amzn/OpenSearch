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

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.opensearch.search.profile.AbstractProfileBreakdown;
import org.opensearch.search.profile.Timer;

import java.io.IOException;
import java.util.Collection;

/**
 * {@link Scorer} wrapper that will compute how much time is spent on moving
 * the iterator, confirming matches and computing scores.
 *
 * @opensearch.internal
 */
final class ProfileScorer extends Scorer {

    private final Scorer scorer;

    private final Timer scoreTimer, nextDocTimer, advanceTimer, matchTimer, shallowAdvanceTimer, computeMaxScoreTimer,
        setMinCompetitiveScoreTimer;

    ProfileScorer(Scorer scorer, AbstractProfileBreakdown profile) throws IOException {
        this.scorer = scorer;
        scoreTimer = profile.getTimer(QueryTimingType.SCORE);
        nextDocTimer = profile.getTimer(QueryTimingType.NEXT_DOC);
        advanceTimer = profile.getTimer(QueryTimingType.ADVANCE);
        matchTimer = profile.getTimer(QueryTimingType.MATCH);
        shallowAdvanceTimer = profile.getTimer(QueryTimingType.SHALLOW_ADVANCE);
        computeMaxScoreTimer = profile.getTimer(QueryTimingType.COMPUTE_MAX_SCORE);
        setMinCompetitiveScoreTimer = profile.getTimer(QueryTimingType.SET_MIN_COMPETITIVE_SCORE);
    }

    @Override
    public int docID() {
        return scorer.docID();
    }

    @Override
    public float score() throws IOException {
        scoreTimer.start();
        try {
            return scorer.score();
        } finally {
            scoreTimer.stop();
        }
    }

    @Override
    public Collection<ChildScorable> getChildren() throws IOException {
        return scorer.getChildren();
    }

    @Override
    public DocIdSetIterator iterator() {
        // Return delegate directly - ZERO per-document overhead
        // NEXT_DOC timing is sacrificed to eliminate wrapper dispatch overhead
        return scorer.iterator();
    }

    @Override
    public TwoPhaseIterator twoPhaseIterator() {
        // Return delegate directly - ZERO per-document overhead
        // MATCH timing is sacrificed to eliminate wrapper dispatch overhead
        return scorer.twoPhaseIterator();
    }

    @Override
    public int advanceShallow(int target) throws IOException {
        shallowAdvanceTimer.start();
        try {
            return scorer.advanceShallow(target);
        } finally {
            shallowAdvanceTimer.stop();
        }
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
        computeMaxScoreTimer.start();
        try {
            return scorer.getMaxScore(upTo);
        } finally {
            computeMaxScoreTimer.stop();
        }
    }

    @Override
    public void setMinCompetitiveScore(float minScore) throws IOException {
        setMinCompetitiveScoreTimer.start();
        try {
            scorer.setMinCompetitiveScore(minScore);
        } finally {
            setMinCompetitiveScoreTimer.stop();
        }
    }

    /**
     * A DocIdSetIterator wrapper that uses boundary-based timing instead of per-call timing.
     * Instead of calling System.nanoTime() on every nextDoc()/advance() call, it records a start
     * time at construction and calculates elapsed time when iteration finishes (NO_MORE_DOCS).
     * This reduces profiling overhead to near-zero (no per-call overhead at all).
     */
    private static class ProfileDocIdSetIterator extends DocIdSetIterator {
        private final DocIdSetIterator delegate;
        private final Timer nextDocTimer;
        private final long startTimeNanos;
        private boolean finished = false;

        ProfileDocIdSetIterator(DocIdSetIterator delegate, Timer nextDocTimer, Timer advanceTimer) {
            this.delegate = delegate;
            this.nextDocTimer = nextDocTimer;  // We'll record all iteration time to nextDoc
            this.startTimeNanos = System.nanoTime();
        }

        @Override
        public int nextDoc() throws IOException {
            int doc = delegate.nextDoc();
            if (doc == NO_MORE_DOCS) {
                finish();
            }
            return doc;
        }

        @Override
        public int advance(int target) throws IOException {
            int doc = delegate.advance(target);
            if (doc == NO_MORE_DOCS) {
                finish();
            }
            return doc;
        }

        @Override
        public int docID() {
            return delegate.docID();
        }

        @Override
        public long cost() {
            return delegate.cost();
        }

        private void finish() {
            if (!finished) {
                long elapsedNanos = System.nanoTime() - startTimeNanos;
                nextDocTimer.addExternalTiming(elapsedNanos, 1);  // Record as single batch
                finished = true;
            }
        }
    }

    /**
     * A TwoPhaseIterator wrapper that uses boundary-based timing for matches().
     * Instead of calling System.nanoTime() on every matches() call, it records a start
     * time at construction and calculates elapsed time when iteration finishes.
     * This reduces profiling overhead to near-zero (no per-call overhead at all).
     */
    private static class ProfileTwoPhaseIterator extends TwoPhaseIterator {
        private final TwoPhaseIterator delegate;
        private final Timer matchTimer;
        private final long startTimeNanos;
        private boolean finished = false;

        ProfileTwoPhaseIterator(TwoPhaseIterator delegate, Timer nextDocTimer, Timer advanceTimer, Timer matchTimer) {
            super(new ProfileDocIdSetIteratorWithCallback(delegate.approximation(), nextDocTimer, advanceTimer, () -> {}));
            this.delegate = delegate;
            this.matchTimer = matchTimer;
            this.startTimeNanos = System.nanoTime();
            // Replace the approximation with one that has our finish callback
            ((ProfileDocIdSetIteratorWithCallback) approximation()).setFinishCallback(this::finishMatch);
        }

        @Override
        public boolean matches() throws IOException {
            return delegate.matches();  // No counting overhead
        }

        @Override
        public float matchCost() {
            return delegate.matchCost();
        }

        private void finishMatch() {
            if (!finished) {
                long elapsedNanos = System.nanoTime() - startTimeNanos;
                matchTimer.addExternalTiming(elapsedNanos, 1);  // Count as 1 batch
                finished = true;
            }
        }
    }

    /**
     * Extended ProfileDocIdSetIterator that supports a finish callback for coordinating
     * with ProfileTwoPhaseIterator's match timing.
     */
    private static class ProfileDocIdSetIteratorWithCallback extends DocIdSetIterator {
        private final DocIdSetIterator delegate;
        private final Timer nextDocTimer;
        private final long startTimeNanos;
        private boolean finished = false;
        private Runnable finishCallback;

        ProfileDocIdSetIteratorWithCallback(DocIdSetIterator delegate, Timer nextDocTimer, Timer advanceTimer, Runnable finishCallback) {
            this.delegate = delegate;
            this.nextDocTimer = nextDocTimer;  // Record all iteration time to nextDoc
            this.startTimeNanos = System.nanoTime();
            this.finishCallback = finishCallback;
        }

        void setFinishCallback(Runnable callback) {
            this.finishCallback = callback;
        }

        @Override
        public int nextDoc() throws IOException {
            int doc = delegate.nextDoc();
            if (doc == NO_MORE_DOCS) {
                finish();
            }
            return doc;
        }

        @Override
        public int advance(int target) throws IOException {
            int doc = delegate.advance(target);
            if (doc == NO_MORE_DOCS) {
                finish();
            }
            return doc;
        }

        @Override
        public int docID() {
            return delegate.docID();
        }

        @Override
        public long cost() {
            return delegate.cost();
        }

        private void finish() {
            if (!finished) {
                long elapsedNanos = System.nanoTime() - startTimeNanos;
                nextDocTimer.addExternalTiming(elapsedNanos, 1);  // Record as single batch
                // Call the callback to finish match timing
                finishCallback.run();
                finished = true;
            }
        }
    }
}
