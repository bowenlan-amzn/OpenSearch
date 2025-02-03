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

/**
 * A composite iterator over multiple DocIdSetIterators where each document
 * belongs to exactly one bucket within a single segment.
 */
public class CompositeDocIdSetIterator extends DocIdSetIterator {
    private final DocIdSetIterator[] iterators;
    private final int numBuckets;
    private int currentDoc = -1;
    private int currentBucket = -1;

    /**
     * Creates a composite view of DocIdSetIterators for a segment where
     * each document belongs to exactly one bucket.
     * @param ordinalToIterator Mapping of bucket ordinal to its DocIdSetIterator
     */
    public CompositeDocIdSetIterator(DocIdSetIterator[] ordinalToIterator) {
        this.iterators = ordinalToIterator;
        this.numBuckets = ordinalToIterator.length;
    }

    @Override
    public int docID() {
        return currentDoc;
    }

    /**
     * Returns the bucket ordinal for the current document.
     * Should only be called when positioned on a valid document.
     * @return bucket ordinal for the current document, or -1 if no current document
     */
    public int getCurrentBucket() {
        return currentBucket;
    }

    @Override
    public int nextDoc() throws IOException {
        return advance(currentDoc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
        if (target == NO_MORE_DOCS) {
            currentDoc = NO_MORE_DOCS;
            currentBucket = -1;
            return NO_MORE_DOCS;
        }

        int minDoc = NO_MORE_DOCS;
        int minDocBucket = -1;

        // Find the iterator with the lowest docID >= target
        for (int bucketOrd = 0; bucketOrd < numBuckets; bucketOrd++) {
            DocIdSetIterator iterator = iterators[bucketOrd];
            if (iterator == null) {
                continue;
            }

            int doc = iterator.docID();
            if (doc < target) {
                doc = iterator.advance(target);
            }

            if (doc < minDoc) {
                minDoc = doc;
                minDocBucket = bucketOrd;
            }
        }

        currentDoc = minDoc;
        currentBucket = minDocBucket;
        return currentDoc;
    }

    @Override
    public long cost() {
        long totalCost = 0;
        for (DocIdSetIterator iterator : iterators) {
            if (iterator != null) {
                totalCost += iterator.cost();
            }
        }
        return totalCost;
    }
}
