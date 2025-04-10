/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.filterrewrite.rangecollector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.DocIdSetBuilder;
import org.opensearch.search.aggregations.BucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.bucket.filterrewrite.FilterRewriteOptimizationContext;
import org.opensearch.search.aggregations.bucket.filterrewrite.Ranges;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Range collector implementation that supports sub-aggregations by collecting doc IDs.
 */
public class SubAggRangeCollector extends AbstractRangeCollector {

    private static final Logger logger = LogManager.getLogger(SubAggRangeCollector.class);

    // private final DocIdSetBuilder[] docIdSetBuilders;
    private DocIdSetBuilder builder = null;
    private final Supplier<DocIdSetBuilder> disBuilderSupplier;

    // private final Map<Long, DocIdSetBuilder> bucketOrdinalToDocIdSetBuilder = new HashMap<>();
    private DocIdSetBuilder.BulkAdder currentAdder;
    private final Function<Integer, Long> getBucketOrd;
    private int lastGrowCount;

    private final BucketCollector collectableSubAggregators;
    private final LeafReaderContext leafCtx;

    public SubAggRangeCollector(
        Ranges ranges,
        BiConsumer<Integer, Integer> incrementRangeDocCount,
        int maxNumNonZeroRange,
        int activeIndex,
        Supplier<DocIdSetBuilder> disBuilderSupplier,
        Function<Integer, Long> getBucketOrd,
        FilterRewriteOptimizationContext.OptimizeResult result,
        BucketCollector collectableSubAggregators,
        LeafReaderContext leafCtx
    ) {
        super(ranges, incrementRangeDocCount, maxNumNonZeroRange, activeIndex, result);
        // this.docIdSetBuilders = new DocIdSetBuilder[ranges.getSize()];
        this.disBuilderSupplier = disBuilderSupplier;
        this.getBucketOrd = getBucketOrd;
        this.collectableSubAggregators = collectableSubAggregators;
        this.leafCtx = leafCtx;
    }

    @Override
    public boolean hasSubAgg() {
        return true;
    }

    @Override
    public void grow(int count) {
        // if (docIdSetBuilders[activeIndex] == null) {
        // docIdSetBuilders[activeIndex] = disBuilderSupplier.get();
        // }
        if (builder == null) {
            builder = disBuilderSupplier.get();
        }
        logger.trace("grow docIdSetBuilder[{}] with count {}", activeIndex, count);
        // currentAdder = docIdSetBuilders[activeIndex].grow(count);
        currentAdder = builder.grow(count);
        lastGrowCount = count;
    }

    @Override
    public void collectDocId(int docId) {
        currentAdder.add(docId);
    }

    @Override
    public void collectDocIdSet(DocIdSetIterator iter) throws IOException {
        currentAdder.add(iter);
    }

    @Override
    public void finalizePreviousRange() {
        if (counter > 0) {
            incrementRangeDocCount.accept(activeIndex, counter);
            counter = 0;
        }

        if (currentAdder != null) {
            assert builder != null;
            long bucketOrd = getBucketOrd.apply(activeIndex);
            logger.trace("finalize docIdSetBuilder[{}] with bucket ordinal {}", activeIndex, bucketOrd);
            // bucketOrdinalToDocIdSetBuilder.put(bucketOrd, docIdSetBuilders[activeIndex]);

            // trigger the sub agg collection
            try {
                DocIdSetIterator iterator = builder.build().iterator();
                // build a new leaf collector for each bucket
                logger.debug("collect sub aggregation when finalize range {}", bucketOrd); // TODO remove
                LeafBucketCollector sub = collectableSubAggregators.getLeafCollector(leafCtx);
                while (iterator.nextDoc() != NO_MORE_DOCS) {
                    int currentDoc = iterator.docID();
                    sub.collect(currentDoc, bucketOrd);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            currentAdder = null;
            builder = null;
        }
    }

    @Override
    public void finalizeDocIdSetBuildersResult() {
        // int maxOrdinal = bucketOrdinalToDocIdSetBuilder.keySet().stream().mapToInt(Long::intValue).max().orElse(0) + 1;
        // DocIdSetBuilder[] builder = new DocIdSetBuilder[maxOrdinal];
        // for (Map.Entry<Long, DocIdSetBuilder> entry : bucketOrdinalToDocIdSetBuilder.entrySet()) {
        // int ordinal = Math.toIntExact(entry.getKey());
        // builder[ordinal] = entry.getValue();
        // }
        // result.builders = builder;
    }

    @Override
    public boolean iterateRangeEnd(byte[] value, boolean inLeaf) {
        boolean shouldStop = super.iterateRangeEnd(value, inLeaf);
        // edge case: if finalizePreviousRange is called within the leaf node
        // currentAdder is reset and grow would not be called immediately
        // here we reuse previous grow count
        if (!shouldStop && inLeaf && currentAdder == null) {
            grow(lastGrowCount);
        }
        return shouldStop;
    }
}
