/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.action;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * A listener for action responses that can handle streaming responses.
 * This interface extends ActionListener to add functionality for handling
 * responses that arrive in multiple batches as part of a stream.
 */
@ExperimentalApi
public interface StreamActionListener<Response> extends ActionListener<Response> {

    /**
     * Stream state for handling partial results
     */
    enum StreamState {
        /**
         * First batch in stream
         */
        STARTED,

        /**
         * Middle batch in stream
         */
        IN_PROGRESS,

        /**
         * Final batch in stream
         */
        COMPLETED
    }

    /**
     * Handle a response with stream state
     * @param response The response data
     * @param state The current stream state
     * @param batchId The batch identifier (incremented for each batch)
     */
    void onStreamResponse(Response response, StreamState state, int batchId);

    /**
     * Support for non-streaming responses, delegates to onStreamResponse with COMPLETED state
     */
    @Override
    default void onResponse(Response response) {
        onStreamResponse(response, StreamState.COMPLETED, 0);
    }
}
