/*
 * Copyright (C) 2014  Ohm Data
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

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
    pipeline.addLast("message-handler", new MessageHandler());
  }

  public void syncOnHandshake() throws InterruptedException, TimeoutException, ExecutionException {
    decoder.syncOnHandshake();
  }
}
