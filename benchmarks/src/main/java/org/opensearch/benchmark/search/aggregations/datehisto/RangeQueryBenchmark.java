/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.search.aggregations.datehisto;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.TotalHitCountCollectorManager;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Index random docs, compare the performance of PointRangeQuery and SortedNumericDocValuesField.newSlowRangeQuery
 */
@Fork(1)
@Warmup(iterations = 10)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class RangeQueryBenchmark {

    // 1M to 10M
    @Param({ "1000000", "2000000", "3000000", "4000000", "5000000", "6000000", "7000000", "8000000", "9000000", "10000000"})
    long docCount;

    private static Path dirPath;
    private static IndexReader reader;
    private static IndexSearcher searcher;
    private static Query prq;
    private static Query dvq;

    @Setup
    public void indexRandomDocs() throws IOException {
        // dir = Files.createTempDirectory(PointRangeQuery.class.getSimpleName());
        dirPath = Paths.get("benchmark-index-" + docCount);

        try (Directory directory = FSDirectory.open(dirPath);
             IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {

            if (DirectoryReader.indexExists(directory)) {
                reader = DirectoryReader.open(directory);
                System.out.println("Existing doc: " + reader.numDocs());
                if (reader.numDocs() == docCount) {
                    System.out.println("Already indexed: " + docCount);
                    setUpSearch();
                    return;
                }
            }

            for (int i = 0; i < docCount; i++) {
                List<IndexableField> doc = new ArrayList<>();
                doc.add(new LongPoint("number", (long)(Math.random() * docCount)));
                doc.add(new SortedNumericDocValuesField("number", (long)(Math.random() * docCount)));
                writer.addDocument(doc);
            }
        }

        try (Directory directory = FSDirectory.open(dirPath)) {
            reader = DirectoryReader.open(directory);
            if (reader.numDocs() != docCount) {
                System.out.println("Confirming doc after indexing: " + reader.numDocs());
                throw new IllegalStateException("Not expected number of documents");
            }
            setUpSearch();
        }
    }

    private void setUpSearch() {
        searcher = new IndexSearcher(reader);
        searcher.setQueryCachingPolicy(new QueryCachingPolicy() {
            @Override
            public void onUse(Query query) { }

            @Override
            public boolean shouldCache(Query query) {
                return false;
            }
        });

        long[] range = new long[]{0, docCount};
        long random = (long)(Math.random() * docCount);
        if (random < docCount / 2) {
            range[0] = random;
        } else {
            range[1] = random;
        }
        prq = LongPoint.newRangeQuery("number", range[0], range[1]);
        dvq = SortedNumericDocValuesField.newSlowRangeQuery("number", range[0], range[1]);
    }

    @TearDown
    public void tearDown() throws IOException {
        // for (String indexFile : FSDirectory.listAll(dir)) {
        //     Files.deleteIfExists(dir.resolve(indexFile));
        // }
        // Files.deleteIfExists(dir);

        reader.close();
    }

    @Benchmark
    public void searchAndCollect(Blackhole bh) throws IOException {
        int count = searcher.search(prq, new TotalHitCountCollectorManager());
        bh.consume(count);
    }

    @Benchmark
    public void iterateDVAndCollect(Blackhole bh) throws IOException {
        int count = searcher.search(dvq, new TotalHitCountCollectorManager());
        bh.consume(count);
    }
}
