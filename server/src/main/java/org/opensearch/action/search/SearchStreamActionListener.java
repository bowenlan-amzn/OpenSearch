/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.core.action.StreamActionListener;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;

/**
 * A specialized StreamActionListener for search operations that tracks shard targets and indices.
 */
abstract class SearchStreamActionListener<T extends SearchPhaseResult> extends SearchActionListener<T> implements StreamActionListener<T> {
    protected SearchStreamActionListener(SearchShardTarget searchShardTarget, int shardIndex) {
        super(searchShardTarget, shardIndex);
    }
    
    /**
     * Handle response with stream state and batch id
     */
    @Override
    public void onStreamResponse(T response, StreamState state, int batchId) {
        // Set batch ID on the response if applicable
        if (response != null) {
            response.setStreamBatchId(batchId);
            response.setShardIndex(requestIndex);
            setSearchShardTarget(response);
            
            // Process response based on stream state
            processStreamResponse(response, state, batchId);
        }
    }
    
    /**
     * Process streamed responses based on their state
     * Implementations should override this method to handle different states
     */
    protected abstract void processStreamResponse(T response, StreamState state, int batchId);
}
