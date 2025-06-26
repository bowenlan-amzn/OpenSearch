/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.action.StreamActionListener;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

/**
 * Wraps a listener to a channel and forwards the response to the channel.
 */
public class StreamChannelActionListener<Response extends TransportResponse, Request extends TransportRequest>
    implements StreamActionListener<Response> {
    private final Logger logger = LogManager.getLogger(StreamChannelActionListener.class);

    private final TransportChannel channel;
    private final Request request;
    private final String actionName;

    public StreamChannelActionListener(TransportChannel channel, String actionName, Request request) {
        this.channel = channel;
        this.request = request;
        this.actionName = actionName;
    }

    @Override
    public void onStreamResponse(Response response) {
        if (response != null) {
            channel.sendResponseBatch(response);
            logger.debug("Sent intermediate response batch");
        }
    }

    @Override
    public void onCompleteResponse(Response response) {
        if (response != null) {
            channel.sendResponseBatch(response);
        }

        channel.completeStream();
        logger.info("Sent final response and completed stream");
    }

    @Override
    public void onFailure(Exception e) {
        try {
            channel.sendResponse(e);
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }
}
