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
package c5db.client.codec.websocket;

import c5db.client.C5Constants;
import c5db.client.FutureBasedMessageHandler;
import c5db.client.generated.Call;
import c5db.client.generated.Response;
import c5db.codec.protostuff.LowCopyProtobufOutputEncoder;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * A simple helper class which initializes a websocket / protocol buffers (protostuff) handler
 * for netty.
 */
public class Initializer extends ChannelInitializer<SocketChannel> {

  private final WebSocketClientHandshaker handShaker;

  private c5db.client.codec.websocket.Decoder decoder;

  public Initializer(WebSocketClientHandshaker handShaker) {
    super();
    this.handShaker = handShaker;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    decoder = new c5db.client.codec.websocket.Decoder(handShaker);
    final ChannelPipeline pipeline = ch.pipeline();

    pipeline.addLast("http-client", new HttpClientCodec());
    pipeline.addLast("aggregator", new HttpObjectAggregator(C5Constants.MAX_RESPONSE_SIZE));

    pipeline.addLast("websocket-decoder", decoder);
    pipeline.addLast("protostuff-decoder", new c5db.codec.protostuff.Decoder<>(Response.getSchema()));

    pipeline.addLast("websocket-encoder", new Encoder(handShaker));
    pipeline.addLast("protostuff-encoder", new LowCopyProtobufOutputEncoder<Call>());

    pipeline.addLast("message-handler", new FutureBasedMessageHandler());
  }

  public void syncOnHandshake() throws InterruptedException, TimeoutException, ExecutionException {
    decoder.syncOnHandshake();
  }
}
