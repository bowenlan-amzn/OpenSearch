/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.analytics.resilience.FlightTransportThreadLeakFilter;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Regression guard for StringView buffer bloat on multi-shard aggregate queries.
 *
 * <p>Background: {@code StringViewArray.slice()} shares ALL backing buffers via Arc.
 * When a hash aggregate emits via {@code EmitTo::All} and slices the result into
 * per-batch chunks, each batch carries the full backing buffer pool — not just the
 * strings it references. Without {@code compact_string_view_columns()} (gc) before
 * FFI export, this caused up to 435x data amplification on coordinator-reduce
 * queries with many distinct string values.
 *
 * <p>This test indexes 10K docs with unique strings > 12 bytes across 2 shards,
 * runs a group-by aggregate with head (forcing multi-batch emission + slicing on
 * the data nodes), and verifies:
 * <ol>
 *   <li>Query produces correct results (functional correctness).</li>
 *   <li>Coordinator's DataFusion memory pool returns to baseline after query
 *       completion (no lingering bloated buffers).</li>
 *   <li>Multiple consecutive queries don't show cumulative memory growth
 *       (proves gc compaction prevents buffer accumulation).</li>
 * </ol>
 *
 * @opensearch.internal
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters(filters = FlightTransportThreadLeakFilter.class)
public class StringViewCompactionIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "stringview_gc_idx";
    private static final int NUM_SHARDS = 2;
    private static final int TOTAL_DOCS = 1_000;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            TestPPLPlugin.class,
            FlightStreamPlugin.class,
            CompositeDataFormatPlugin.class,
            MockCommitterEnginePlugin.class
        );
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            classpathPlugin(AnalyticsPlugin.class, Collections.emptyList()),
            classpathPlugin(ParquetDataFormatPlugin.class, Collections.emptyList()),
            classpathPlugin(DataFusionPlugin.class, List.of(AnalyticsPlugin.class.getName()))
        );
    }

    private static PluginInfo classpathPlugin(Class<? extends Plugin> pluginClass, List<String> extendedPlugins) {
        return new PluginInfo(
            pluginClass.getName(),
            "classpath plugin",
            "NA",
            Version.CURRENT,
            "1.8",
            pluginClass.getName(),
            null,
            extendedPlugins,
            false
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .put(FeatureFlags.STREAM_TRANSPORT, true)
            .build();
    }

    private void createAndSeedIndex() {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(INDEX)
            .setSettings(indexSettings)
            .setMapping("str_col", "type=keyword", "val", "type=integer")
            .get();
        assertTrue("index creation must be acknowledged", response.isAcknowledged());
        ensureGreen(INDEX);

        for (int i = 0; i < TOTAL_DOCS; i++) {
            String longString = String.format("unique_string_value_%010d_pad", i);
            client().prepareIndex(INDEX).setId(String.valueOf(i))
                .setSource("str_col", longString, "val", i % 100)
                .get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareFlush(INDEX).get();

        long expectedSum = 0;
        for (int i = 0; i < TOTAL_DOCS; i++) {
            expectedSum += i % 100;
        }
        final long expected = expectedSum;
        try {
            assertBusy(() -> {
                PPLResponse r = executePPL("source = " + INDEX + " | stats sum(val) as total");
                long actual = ((Number) r.getRows().get(0)[r.getColumns().indexOf("total")]).longValue();
                assertEquals("seed not yet visible to analytics path", expected, actual);
            }, 30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("createAndSeedIndex: timed out waiting for seed durability", e);
        }
    }

    /**
     * Core regression test: a multi-shard group-by with head produces a small result
     * set from a large number of distinct string values. Before the gc() fix, the
     * flight transfer would carry all 10K distinct strings' backing buffers even for
     * a 5-row result.
     */
    public void testGroupByWithHeadProducesCompactTransfer() {
        createAndSeedIndex();

        DataFusionService dfs = internalCluster().getInstance(
            DataFusionService.class, internalCluster().getNodeNames()[0]
        );
        long memBefore = dfs.getMemoryPoolUsage();

        PPLResponse response = executePPL(
            "source = " + INDEX + " | stats count() by str_col | head 5"
        );

        assertEquals("head 5 must produce exactly 5 rows", 5, response.getRows().size());
        assertTrue("response must have str_col column", response.getColumns().contains("str_col"));
        assertTrue("response must have count() column", response.getColumns().contains("count()"));

        for (Object[] row : response.getRows()) {
            int strColIdx = response.getColumns().indexOf("str_col");
            String str = (String) row[strColIdx];
            assertTrue(
                "returned strings must be > 12 bytes (stored in backing buffers)",
                str.length() > 12
            );
        }

        long memAfter = dfs.getMemoryPoolUsage();
        long drift = memAfter - memBefore;
        logger.info(
            "StringView compaction test: memBefore={} memAfter={} drift={}",
            memBefore, memAfter, drift
        );
        assertTrue(
            "Memory pool must return to near-baseline after query (drift=" + drift + " bytes). "
                + "Large drift indicates StringView buffers were not compacted before FFI export.",
            drift < 1024 * 1024
        );
    }

    /**
     * Repeated queries must not show cumulative memory growth — proves that gc()
     * compaction prevents buffer accumulation across multiple query executions.
     */
    public void testRepeatedGroupByQueriesNoMemoryGrowth() {
        createAndSeedIndex();

        DataFusionService dfs = internalCluster().getInstance(
            DataFusionService.class, internalCluster().getNodeNames()[0]
        );

        executePPL("source = " + INDEX + " | stats count() by str_col | head 5");
        long baseline = dfs.getMemoryPoolUsage();

        final int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            PPLResponse r = executePPL(
                "source = " + INDEX + " | stats count() by str_col | head 5"
            );
            assertEquals("each iteration must return 5 rows", 5, r.getRows().size());
        }

        long afterAll = dfs.getMemoryPoolUsage();
        long growth = afterAll - baseline;
        logger.info(
            "Repeated query test: baseline={} afterAll={} growth={} ({} iterations)",
            baseline, afterAll, growth, iterations
        );
        assertTrue(
            "Memory must not grow across " + iterations + " repeated queries (growth=" + growth
                + " bytes). Cumulative growth indicates gc() is not compacting buffers.",
            growth < 2 * 1024 * 1024
        );
    }

    private PPLResponse executePPL(String ppl) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(ppl)).actionGet();
    }
}
