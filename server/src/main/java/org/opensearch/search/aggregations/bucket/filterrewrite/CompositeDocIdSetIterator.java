/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.filterrewrite;

import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;
import java.util.Arrays;

/**
 * A composite view of multiple DocIdSetIterators from single segment
 */
public class CompositeDocIdSetIterator extends DocIdSetIterator {
    private final DocIdSetIterator[] iterators;
    private final int[] currentDocs;  // Current docId for each iterator
    private final boolean[] exhausted; // Track if each iterator is exhausted
    private int currentDoc = -1;      // Current doc for this composite iterator
    private final int numIterators;

    /**
     * Creates a composite view of multiple DocIdSetIterators
     * @param ordinalToIterator Mapping of bucket ordinal to its DocIdSetIterator
     * @param maxOrdinal The maximum bucket ordinal (exclusive)
     */
    public CompositeDocIdSetIterator(DocIdSetIterator[] ordinalToIterator, int maxOrdinal) {
        this.iterators = Arrays.copyOf(ordinalToIterator, maxOrdinal);
        this.numIterators = maxOrdinal;
        this.currentDocs = new int[maxOrdinal];
        this.exhausted = new boolean[maxOrdinal];

        // Initialize currentDocs array to -1 for all iterators
        Arrays.fill(currentDocs, -1);
    }

    @Override
    public int docID() {
        return currentDoc;
    }

    @Override
    public int nextDoc() throws IOException {
        return advance(currentDoc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
        if (target == NO_MORE_DOCS) {
            currentDoc = NO_MORE_DOCS;
            return NO_MORE_DOCS;
        }

        int minDoc = NO_MORE_DOCS;

        // Advance all iterators that are behind target
        for (int i = 0; i < numIterators; i++) {
            if (iterators[i] == null) {
                exhausted[i] = true;
                continue;
            }

            if (!exhausted[i] && currentDocs[i] < target) {
                int doc = iterators[i].advance(target);
                if (doc == NO_MORE_DOCS) {
                    exhausted[i] = true;
                } else {
                    currentDocs[i] = doc;
                    minDoc = Math.min(minDoc, doc);
                }
            } else if (!exhausted[i]) {
                minDoc = Math.min(minDoc, currentDocs[i]);
            }
        }

        currentDoc = minDoc;
        return currentDoc;
    }

    @Override
    public long cost() {
        long maxCost = 0;
        for (DocIdSetIterator iterator : iterators) {
            if (iterator != null) {
                maxCost = Math.max(maxCost, iterator.cost());
            }
        }
        return maxCost;
    }

    /**
     * Checks if a specific bucket matches the current document
     * @param ordinal The bucket ordinal to check
     * @return true if the bucket matches the current document
     */
    public boolean matches(int ordinal) {
        if (ordinal >= numIterators || currentDoc == NO_MORE_DOCS) {
            return false;
        }
        return !exhausted[ordinal] && currentDocs[ordinal] == currentDoc;
    }

    /**
     * Gets a bit set representing all buckets that match the current document
     * @return A long where each bit position represents whether the corresponding bucket matches
     */
    public long getMatchingBuckets() {
        if (currentDoc == NO_MORE_DOCS || numIterators > 64) {
            return 0L;
        }

        long result = 0L;
        for (int i = 0; i < numIterators; i++) {
            if (matches(i)) {
                result |= 1L << i;
            }
        }
        return result;
    }
}
