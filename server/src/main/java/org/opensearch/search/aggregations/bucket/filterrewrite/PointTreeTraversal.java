/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.filterrewrite;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.DocIdSetBuilder;
import org.opensearch.common.CheckedRunnable;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Utility class for traversing a {@link PointValues.PointTree} and collecting document counts for the ranges.
 *
 * <p>The main entry point is the {@link #multiRangesTraverse} method
 *
 * <p>The class uses a {@link RangeCollectorForPointTree} to keep track of the active ranges and
 * determine which parts of the tree to visit. The {@link
 * PointValues.IntersectVisitor} implementation is responsible for the actual visitation and
 * document count collection.
 */
final class PointTreeTraversal {
    private PointTreeTraversal() {}

    private static final Logger logger = LogManager.getLogger(Helper.loggerName);

    /**
     * Traverses the given {@link PointValues.PointTree} and collects document counts for the intersecting ranges.
     *
     * @param tree                 the point tree to traverse
     * @param ranges               the set of ranges to intersect with
     * @param incrementDocCount    a callback to increment the document count for a range bucket
     * @param maxNumNonZeroRanges  the maximum number of non-zero ranges to collect
     * @return a {@link FilterRewriteOptimizationContext.DebugInfo} object containing debug information about the traversal
     */
    static FilterRewriteOptimizationContext.DebugInfo multiRangesTraverse(
        final PointValues.PointTree tree,
        final Ranges ranges,
        final BiConsumer<Integer, Integer> incrementDocCount,
        final int maxNumNonZeroRanges,
        Supplier<DocIdSetBuilder> disBuilderSupplier
    ) throws IOException {
        FilterRewriteOptimizationContext.DebugInfo debugInfo = new FilterRewriteOptimizationContext.DebugInfo();
        int activeIndex = ranges.firstRangeIndex(tree.getMinPackedValue(), tree.getMaxPackedValue());
        if (activeIndex < 0) {
            logger.debug("No ranges match the query, skip the fast filter optimization");
            return debugInfo;
        }
        RangeCollectorForPointTree collector = new RangeCollectorForPointTree(
            incrementDocCount,
            maxNumNonZeroRanges,
            ranges,
            activeIndex,
            disBuilderSupplier
        );

        PointValues.IntersectVisitor visitor = getIntersectVisitor(collector);
        try {
            intersectWithRanges(visitor, tree, collector, debugInfo);
        } catch (CollectionTerminatedException e) {
            logger.debug("Early terminate since no more range to collect");
        }
        collector.finalizePreviousRange();

        DocIdSetBuilder[] builders = collector.docIdSetBuilders;
        logger.debug("length of docIdSetBuilders: {}", builders.length);
        for (int i = 0; i < builders.length; i++) {
            if (builders[i] != null) {
                int count = 0;
                DocIdSetIterator iterator = builders[i].build().iterator();
                while (iterator.nextDoc() != NO_MORE_DOCS) {
                    count++;
                }
                logger.debug(" docIdSetBuilder[{}] disi has documents: {}", i, count);
            }
        }

        return debugInfo;
    }

    private static void intersectWithRanges(
        PointValues.IntersectVisitor visitor,
        PointValues.PointTree pointTree,
        RangeCollectorForPointTree collector,
        FilterRewriteOptimizationContext.DebugInfo debug
    ) throws IOException {
        PointValues.Relation r = visitor.compare(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());

        switch (r) {
            case CELL_INSIDE_QUERY:
                collector.countNode((int) pointTree.size());
                pointTree.visitDocIDs(visitor);
                debug.visitInner();
                break;
            case CELL_CROSSES_QUERY:
                if (pointTree.moveToChild()) {
                    do {
                        intersectWithRanges(visitor, pointTree, collector, debug);
                    } while (pointTree.moveToSibling());
                    pointTree.moveToParent();
                } else {
                    pointTree.visitDocValues(visitor);
                    debug.visitLeaf();
                }
                break;
            case CELL_OUTSIDE_QUERY:
        }
    }

    private static PointValues.IntersectVisitor getIntersectVisitor(RangeCollectorForPointTree collector) {
        return new PointValues.IntersectVisitor() {

            @Override
            public void grow(int count) {
                collector.grow(count);
            }

            @Override
            public void visit(int docID) {
                // this branch should be unreachable
                // throw new UnsupportedOperationException(
                // "This IntersectVisitor does not perform any actions on a " + "docID=" + docID + " node being visited"
                // );
                collector.collectDocId(docID);
            }

            @Override
            public void visit(DocIdSetIterator iterator) throws IOException {
                collector.collectDocIdSet(iterator);
            }

            @Override
            public void visit(int docID, byte[] packedValue) throws IOException {
                visitPoints(packedValue, collector::count);

                collector.collectDocId(docID);
            }

            @Override
            public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
                visitPoints(packedValue, () -> {
                    for (int doc = iterator.nextDoc(); doc != NO_MORE_DOCS; doc = iterator.nextDoc()) {
                        collector.count();
                    }
                });

                collector.collectDocIdSet(iterator);
            }

            private void visitPoints(byte[] packedValue, CheckedRunnable<IOException> collect) throws IOException {
                if (!collector.withinUpperBound(packedValue)) {
                    collector.finalizePreviousRange();
                    if (collector.iterateRangeEnd(packedValue)) {
                        throw new CollectionTerminatedException();
                    }
                }

                if (collector.withinRange(packedValue)) {
                    collect.run();
                }
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                // try to find the first range that may collect values from this cell
                if (!collector.withinUpperBound(minPackedValue)) {
                    collector.finalizePreviousRange();
                    if (collector.iterateRangeEnd(minPackedValue)) {
                        throw new CollectionTerminatedException();
                    }
                }
                // after the loop, min < upper
                // cell could be outside [min max] lower
                if (!collector.withinLowerBound(maxPackedValue)) {
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;
                }
                if (collector.withinRange(minPackedValue) && collector.withinRange(maxPackedValue)) {
                    return PointValues.Relation.CELL_INSIDE_QUERY;
                }
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
        };
    }

    private static class RangeCollectorForPointTree {
        private final BiConsumer<Integer, Integer> incrementRangeDocCount;
        private int counter = 0;

        private final Ranges ranges;
        private int activeIndex;
        private final DocIdSetBuilder[] docIdSetBuilders;
        private final Supplier<DocIdSetBuilder> disBuilderSupplier;
        private DocIdSetBuilder.BulkAdder currentAdder;

        private int visitedRange = 0;
        private final int maxNumNonZeroRange;

        public RangeCollectorForPointTree(
            BiConsumer<Integer, Integer> incrementRangeDocCount,
            int maxNumNonZeroRange,
            Ranges ranges,
            int activeIndex,
            Supplier<DocIdSetBuilder> disBuilderSupplier
        ) {
            this.incrementRangeDocCount = incrementRangeDocCount;
            this.maxNumNonZeroRange = maxNumNonZeroRange;
            this.ranges = ranges;
            this.activeIndex = activeIndex;
            this.docIdSetBuilders = new DocIdSetBuilder[ranges.size];
            this.disBuilderSupplier = disBuilderSupplier;
        }

        private void count() {
            counter++;
        }

        private void collectDocId(int docId) {
            // if (docIdSetBuilders[activeIndex] == null) {
            //     // TODO hard code for now, should be controlled by intersector grow
            //     docIdSetBuilders[activeIndex] = disBuilderSupplier.get();
            //     currentAdder = docIdSetBuilders[activeIndex].grow(1000);
            // }
            currentAdder.add(docId);
        }

        private void collectDocIdSet(DocIdSetIterator iter) throws IOException {
            // if (docIdSetBuilders[activeIndex] == null) {
            //     // TODO hard code for now, should be controlled by intersector grow
            //     docIdSetBuilders[activeIndex] = disBuilderSupplier.get();
            //     currentAdder = docIdSetBuilders[activeIndex].grow(1000);
            // }
            currentAdder.add(iter);
        }

        private void grow(int count) {
            if (docIdSetBuilders[activeIndex] == null) {
                docIdSetBuilders[activeIndex] = disBuilderSupplier.get();
            }
            logger.debug("grow docIdSetBuilder[{}] with count 200_000", activeIndex);
            currentAdder = docIdSetBuilders[activeIndex].grow(200_000);
        }

        private void countNode(int count) {
            counter += count;
        }

        private void finalizePreviousRange() {
            if (counter > 0) {
                incrementRangeDocCount.accept(activeIndex, counter);
                counter = 0;
            }
        }

        /**
         * @return true when iterator exhausted or collect enough non-zero ranges
         */
        private boolean iterateRangeEnd(byte[] value) {
            // the new value may not be contiguous to the previous one
            // so try to find the first next range that cross the new value
            while (!withinUpperBound(value)) {
                if (++activeIndex >= ranges.size) {
                    return true;
                }
            }
            visitedRange++;
            return visitedRange > maxNumNonZeroRange;
        }

        private boolean withinLowerBound(byte[] value) {
            return Ranges.withinLowerBound(value, ranges.lowers[activeIndex]);
        }

        private boolean withinUpperBound(byte[] value) {
            return Ranges.withinUpperBound(value, ranges.uppers[activeIndex]);
        }

        private boolean withinRange(byte[] value) {
            return withinLowerBound(value) && withinUpperBound(value);
        }
    }
}
