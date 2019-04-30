/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.net;

import java.io.IOException;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.async.InboundMessageCallbacks;

/**
 * Sends a response for an incoming message with a matching {@link Matcher}.
 * The actual behavior by any instance of this class can be inspected by
 * interacting with the returned {@link MockMessagingSpy}.
 */
public class MatcherResponse
{
    private static final Logger logger = LoggerFactory.getLogger(MatcherResponse.class);

    private final Matcher<?> matcher;
    private final Set<Long> sendResponses = new HashSet<>();
    private final MockMessagingSpy spy = new MockMessagingSpy();
    private final AtomicInteger limitCounter = new AtomicInteger(Integer.MAX_VALUE);
    private MessageSink.OutboundSink sink;

    MatcherResponse(Matcher<?> matcher)
    {
        this.matcher = matcher;
    }

    /**
     * Do not create any responses for intercepted outbound messages.
     */
    public MockMessagingSpy dontReply()
    {
        return respond((Message<?>)null);
    }

    /**
     * Respond with provided message in response to each intercepted outbound message.
     * @param message   the message to use as mock response from the cluster
     */
    public MockMessagingSpy respond(Message<?> message)
    {
        return respondN(message, Integer.MAX_VALUE);
    }

    /**
     * Respond a limited number of times with the provided message in response to each intercepted outbound message.
     * @param response  the message to use as mock response from the cluster
     * @param limit     number of times to respond with message
     */
    public MockMessagingSpy respondN(final Message<?> response, int limit)
    {
        return respondN((in, to) -> response, limit);
    }

    /**
     * Respond with the message created by the provided function that will be called with each intercepted outbound message.
     * @param fnResponse    function to call for creating response based on intercepted message and target address
     */
    public <T, S> MockMessagingSpy respond(BiFunction<Message<T>, InetAddressAndPort, Message<S>> fnResponse)
    {
        return respondN(fnResponse, Integer.MAX_VALUE);
    }

    /**
     * Respond with message wrapping the payload object created by provided function called for each intercepted outbound message.
     * The target address from the intercepted message will automatically be used as the created message's sender address.
     * @param fnResponse    function to call for creating payload object based on intercepted message and target address
     * @param verb          verb to use for response message
     */
    public <T, S> MockMessagingSpy respondWithPayloadForEachReceiver(Function<Message<T>, S> fnResponse, Verb verb)
    {
        return respondNWithPayloadForEachReceiver(fnResponse, verb, Integer.MAX_VALUE);
    }

    /**
     * Respond a limited number of times with message wrapping the payload object created by provided function called for
     * each intercepted outbound message. The target address from the intercepted message will automatically be used as the
     * created message's sender address.
     * @param fnResponse    function to call for creating payload object based on intercepted message and target address
     * @param verb          verb to use for response message
     */
    public <T, S> MockMessagingSpy respondNWithPayloadForEachReceiver(Function<Message<T>, S> fnResponse, Verb verb, int limit)
    {
        return respondN((Message<T> msg, InetAddressAndPort to) -> {
                    S payload = fnResponse.apply(msg);
                    if (payload == null)
                        return null;
                    else
                        return Message.builder(verb, payload).from(to).build();
                },
                limit);
    }

    /**
     * Responds to each intercepted outbound message by creating a response message wrapping the next element consumed
     * from the provided queue. No response will be send when the queue has been exhausted.
     * @param cannedResponses   prepared payload messages to use for responses
     * @param verb              verb to use for response message
     */
    public <T, S> MockMessagingSpy respondWithPayloadForEachReceiver(Queue<S> cannedResponses, Verb verb)
    {
        return respondWithPayloadForEachReceiver((Message<T> msg) -> cannedResponses.poll(), verb);
    }

    /**
     * Responds to each intercepted outbound message by creating a response message wrapping the next element consumed
     * from the provided queue. This method will block until queue elements are available.
     * @param cannedResponses   prepared payload messages to use for responses
     * @param verb              verb to use for response message
     */
    public <T, S> MockMessagingSpy respondWithPayloadForEachReceiver(BlockingQueue<S> cannedResponses, Verb verb)
    {
        return respondWithPayloadForEachReceiver((Message<T> msg) -> {
            try
            {
                return cannedResponses.take();
            }
            catch (InterruptedException e)
            {
                return null;
            }
        }, verb);
    }

    /**
     * Respond a limited number of times with the message created by the provided function that will be called with
     * each intercepted outbound message.
     * @param fnResponse    function to call for creating response based on intercepted message and target address
     */
    public <T, S> MockMessagingSpy respondN(BiFunction<Message<T>, InetAddressAndPort, Message<S>> fnResponse, int limit)
    {
        limitCounter.set(limit);

        assert sink == null: "destroy() must be called first to register new response";

        sink = new MessageSink.OutboundSink()
        {
            public boolean allowOutbound(Message message, InetAddressAndPort to)
            {
                // prevent outgoing message from being send in case matcher indicates a match
                // and instead send the mocked response
                if (matcher.matches(message, to))
                {
                    spy.matchingMessage(message);

                    if (limitCounter.decrementAndGet() < 0)
                        return false;

                    synchronized (sendResponses)
                    {
                        if (message.hasId())
                        {
                            assert !sendResponses.contains(message.id()) : "ID re-use for outgoing message";
                            sendResponses.add(message.id());
                        }
                    }

                    // create response asynchronously to match request/response communication execution behavior
                    new Thread(() ->
                    {
                        Message<?> response = fnResponse.apply(message, to);
                        if (response != null)
                        {
                            CallbackInfo cb = MessagingService.instance().callbacks.get(message.id());
                            if (cb != null)
                                cb.callback.response(response);
                            else
                                processResponse(response);

                            spy.matchingResponse(response);
                        }
                    }).start();

                    return false;
                }
                return true;
            }
        };
        MessagingService.instance().messageSink.addOutbound(sink);

        return spy;
    }

    private void processResponse(Message<?> message)
    {
        if (!MessagingService.instance().messageSink.allowInbound(message))
            return;

        StageManager.getStage(message.verb().stage).execute(() -> {
            try
            {
                message.verb().handler().doVerb((Message<Object>)message);
            }
            catch (IOException e)
            {
                //
            }
        });
    }

    /**
     * Stops currently registered response from being send.
     */
    public void destroy()
    {
        MessagingService.instance().messageSink.removeOutbound(sink);
    }
}
