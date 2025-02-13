/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.range;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.InternalAutoDateHistogram;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;

public class RangeAutoDateAggregatorTests extends AggregatorTestCase {
    private String longField = "metric";
    private String dateField = "timestamp";
    private Query matchAllQuery = new MatchAllDocsQuery();
    private NumberFieldMapper.NumberFieldType longFT = new NumberFieldMapper.NumberFieldType(longField, NumberFieldMapper.NumberType.LONG);
    private DateFieldMapper.DateFieldType dateFT = aggregableDateFieldType(false, true);
    private NumberFieldMapper.NumberType numberType = longFT.numberType();
    private String rangeAggName = "range";
    private String autoDateAggName = "auto";

    public void testExample() throws IOException {
        Instant base = Instant.parse("2020-03-01T00:00:00Z");
        List<TestDoc> docs = Collections.singletonList(new TestDoc(1, base));

        RangeAggregationBuilder rangeAggregationBuilder = new RangeAggregationBuilder(rangeAggName).field(longField)
            .addRange(0, 2)
            .subAggregation(new AutoDateHistogramAggregationBuilder(autoDateAggName).field(dateField).setNumBuckets(3));

        InternalRange result = executeAggregation(docs, rangeAggregationBuilder);

        // Verify results
        List<? extends InternalRange.Bucket> buckets = result.getBuckets();
        assertEquals(1, buckets.size());
        InternalRange.Bucket firstBucket = buckets.get(0);
        assertEquals(1, firstBucket.getDocCount());
        InternalAutoDateHistogram firstAuto = firstBucket.getAggregations().get(autoDateAggName);
        assertEquals(1, firstAuto.getBuckets().size());
    }

    public void testOverlap() throws IOException {
        Instant base = Instant.parse("2020-03-01T00:00:00Z");
        List<TestDoc> docs = List.of(
            new TestDoc(1, base),
            new TestDoc(1, base.plusSeconds(3600)),
            new TestDoc(1, base.plusSeconds(7200)),
            new TestDoc(3, base.plusSeconds(3600))
        );

        RangeAggregationBuilder rangeAggregationBuilder = new RangeAggregationBuilder(rangeAggName).field(longField)
            .addRange(0, 2)
            .subAggregation(new AutoDateHistogramAggregationBuilder(autoDateAggName).field(dateField).setNumBuckets(3));

        InternalRange result = executeAggregation(docs, rangeAggregationBuilder);

        // Verify results
        List<? extends InternalRange.Bucket> buckets = result.getBuckets();
        assertEquals(1, buckets.size());
        InternalRange.Bucket firstBucket = buckets.get(0);
        assertEquals(3, firstBucket.getDocCount());
        InternalAutoDateHistogram firstAuto = firstBucket.getAggregations().get(autoDateAggName);
        assertEquals(3, firstAuto.getBuckets().size());
    }

    private InternalRange executeAggregation(List<TestDoc> docs, RangeAggregationBuilder aggregationBuilder) throws IOException {
        try (Directory directory = setupIndex(docs)) {
            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                return executeAggregationOnReader(indexReader, aggregationBuilder);
            }
        }
    }

    private Directory setupIndex(List<TestDoc> docs) throws IOException {
        Directory directory = newDirectory();
        try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig().setCodec(TestUtil.getDefaultCodec()))) {
            for (TestDoc doc : docs) {
                indexWriter.addDocument(doc.toDocument());
            }
        }
        return directory;
    }

    private InternalRange executeAggregationOnReader(DirectoryReader indexReader, RangeAggregationBuilder aggregationBuilder)
        throws IOException {
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);

        MultiBucketConsumerService.MultiBucketConsumer bucketConsumer = createBucketConsumer();
        SearchContext searchContext = createSearchContext(
            indexSearcher,
            createIndexSettings(),
            matchAllQuery,
            bucketConsumer,
            longFT,
            dateFT
        );
        RangeAggregator aggregator = createAggregator(aggregationBuilder, searchContext);

        // Execute aggregation
        aggregator.preCollection();
        indexSearcher.search(matchAllQuery, aggregator);
        aggregator.postCollection();

        // Reduce results
        InternalRange topLevel = (InternalRange) aggregator.buildTopLevel();
        MultiBucketConsumerService.MultiBucketConsumer reduceBucketConsumer = createReduceBucketConsumer();
        InternalAggregation.ReduceContext context = createReduceContext(aggregator, reduceBucketConsumer);

        InternalRange result = (InternalRange) topLevel.reduce(Collections.singletonList(topLevel), context);
        doAssertReducedMultiBucketConsumer(result, reduceBucketConsumer);

        return result;
    }

    private MultiBucketConsumerService.MultiBucketConsumer createBucketConsumer() {
        return new MultiBucketConsumerService.MultiBucketConsumer(
            DEFAULT_MAX_BUCKETS,
            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
        );
    }

    private MultiBucketConsumerService.MultiBucketConsumer createReduceBucketConsumer() {
        return new MultiBucketConsumerService.MultiBucketConsumer(
            Integer.MAX_VALUE,
            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
        );
    }

    private InternalAggregation.ReduceContext createReduceContext(
        RangeAggregator aggregator,
        MultiBucketConsumerService.MultiBucketConsumer reduceBucketConsumer
    ) {
        return InternalAggregation.ReduceContext.forFinalReduction(
            aggregator.context().bigArrays(),
            getMockScriptService(),
            reduceBucketConsumer,
            PipelineAggregator.PipelineTree.EMPTY
        );
    }

    private class TestDoc {
        private final long metric;
        private final Instant timestamp;

        public TestDoc(long metric, Instant timestamp) {
            this.metric = metric;
            this.timestamp = timestamp;
        }

        public ParseContext.Document toDocument() {
            ParseContext.Document doc = new ParseContext.Document();

            List<Field> fieldList = numberType.createFields(longField, metric, true, true, false);
            for (Field fld : fieldList)
                doc.add(fld);
            doc.add(new SortedNumericDocValuesField(dateField, dateFT.parse(timestamp.toString())));

            return doc;
        }
    }

    protected final DateFieldMapper.DateFieldType aggregableDateFieldType(boolean useNanosecondResolution, boolean isSearchable) {
        return new DateFieldMapper.DateFieldType(
            "timestamp",
            isSearchable,
            false,
            true,
            DateFieldMapper.getDefaultDateTimeFormatter(),
            useNanosecondResolution ? DateFieldMapper.Resolution.NANOSECONDS : DateFieldMapper.Resolution.MILLISECONDS,
            null,
            Collections.emptyMap()
        );
    }

}
