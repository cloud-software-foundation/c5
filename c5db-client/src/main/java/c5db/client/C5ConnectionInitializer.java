/*
 * Copyright 2014 WANdisco
 *
 *  WANdisco licenses this file to you under the Apache License,
 *  version 2.0 (the "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations
 *  under the License.
 */
package c5db.client;

import c5db.client.codec.WebsocketProtostuffDecoder;
import c5db.client.codec.WebsocketProtostuffEncoder;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * A simple helper class which initializes a websocket / protocol buffers (protostuff) handler
 * for netty.
 */
class C5ConnectionInitializer extends ChannelInitializer<SocketChannel> {

  private final WebSocketClientHandshaker handShaker;
  private WebsocketProtostuffDecoder decoder;

  public C5ConnectionInitializer(WebSocketClientHandshaker handShaker) {
    super();
    this.handShaker = handShaker;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    decoder = new WebsocketProtostuffDecoder(handShaker);
    final ChannelPipeline pipeline = ch.pipeline();
    pipeline.addLast("http-client", new HttpClientCodec());
    pipeline.addLast("aggregator", new HttpObjectAggregator(C5Constants.MAX_RESPONSE_SIZE));
    pipeline.addLast("websec-codec", new WebsocketProtostuffEncoder(handShaker));
    pipeline.addLast("websocket-aggregator", new WebSocketFrameAggregator(C5Constants.MAX_RESPONSE_SIZE));
    pipeline.addLast("message-codec", decoder);
    pipeline.addLast("message-handler", new FutureBasedMessageHandler());
  }

  public void syncOnHandshake() throws InterruptedException, TimeoutException, ExecutionException {
    decoder.syncOnHandshake();
  }
}
