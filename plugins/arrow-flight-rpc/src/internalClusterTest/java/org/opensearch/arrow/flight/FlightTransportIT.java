/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.arrow.flight.bootstrap.FlightStreamPlugin;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.Max;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.BeforeClass;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, minNumDataNodes = 3, maxNumDataNodes = 3)
public class FlightTransportIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(FlightStreamPlugin.class);
    }

    @BeforeClass
    public static void setupSysProperties() {
        System.setProperty("io.netty.allocator.numDirectArenas", "1");
        System.setProperty("io.netty.noUnsafe", "false");
        System.setProperty("io.netty.tryUnsafe", "true");
        System.setProperty("io.netty.tryReflectionSetAccessible", "true");
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        internalCluster().ensureAtLeastNumDataNodes(3);

        Settings indexSettings = Settings.builder()
            .put("index.number_of_shards", 1)    // Number of primary shards
            .put("index.number_of_replicas", 0)  // Number of replica shards
            .put("index.search.concurrent_segment_search.mode", "none")
            .build();

        CreateIndexRequest createIndexRequest = new CreateIndexRequest("index").settings(indexSettings);
        createIndexRequest.mapping(
            "{\n"
                + "  \"properties\": {\n"
                + "    \"field1\": { \"type\": \"keyword\" },\n"
                + "    \"field2\": { \"type\": \"integer\" }\n"
                + "  }\n"
                + "}", XContentType.JSON);
        CreateIndexResponse createIndexResponse = client().admin().indices().create(createIndexRequest).actionGet();
        assertTrue(createIndexResponse.isAcknowledged());
        client().admin().cluster().prepareHealth("index").setWaitForGreenStatus().setTimeout(TimeValue.timeValueSeconds(30)).get();
        BulkRequest bulkRequest = new BulkRequest();

        bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value1", "field2", 42));
        bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value2", "field2", 43));
        bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value3", "field2", 44));
        bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value1", "field2", 42));
        bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value2", "field2", 43));
        bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value3", "field2", 44));

        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertFalse(bulkResponse.hasFailures()); // Verify ingestion was successful
        client().admin().indices().refresh(new RefreshRequest("index")).actionGet();
        ensureSearchable("index");
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testStreamingAggregation() throws Exception {
        TermsAggregationBuilder agg = terms("agg1").field("field1")
            .subAggregation(AggregationBuilders.max("agg2").field("field2"));
        ActionFuture<SearchResponse> future = client().prepareStreamSearch("index")
            .addAggregation(agg)
            .setSize(0)
            .setRequestCache(false)
            .execute();
        SearchResponse resp = future.actionGet();
        assertNotNull(resp);
        assertEquals(1, resp.getTotalShards());
        assertEquals(6, resp.getHits().getTotalHits().value());
        StringTerms agg1 = (StringTerms) resp.getAggregations().asMap().get("agg1");
        List<StringTerms.Bucket> buckets = agg1.getBuckets();
        assertEquals(3, buckets.size());

        // Validate all buckets - each should have 2 documents
        for (StringTerms.Bucket bucket : buckets) {
            assertEquals(2, bucket.getDocCount());
            assertNotNull(bucket.getAggregations().get("agg2"));
        }
        buckets.sort(Comparator.comparing(StringTerms.Bucket::getKeyAsString));

        StringTerms.Bucket bucket1 = buckets.get(0);
        assertEquals("value1", bucket1.getKeyAsString());
        assertEquals(2, bucket1.getDocCount());
        Max maxAgg1 = (Max) bucket1.getAggregations().get("agg2");
        assertEquals(42.0, maxAgg1.getValue(), 0.001);

        StringTerms.Bucket bucket2 = buckets.get(1);
        assertEquals("value2", bucket2.getKeyAsString());
        assertEquals(2, bucket2.getDocCount());
        Max maxAgg2 = (Max) bucket2.getAggregations().get("agg2");
        assertEquals(43.0, maxAgg2.getValue(), 0.001);

        StringTerms.Bucket bucket3 = buckets.get(2);
        assertEquals("value3", bucket3.getKeyAsString());
        assertEquals(2, bucket3.getDocCount());
        Max maxAgg3 = (Max) bucket3.getAggregations().get("agg2");
        assertEquals(44.0, maxAgg3.getValue(), 0.001);

        for (SearchHit hit : resp.getHits().getHits()) {
            assertNotNull(hit.getSourceAsString());
        }
    }
}
