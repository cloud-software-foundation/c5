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
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.mortbay.log.Log;

import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class C5ConnectionInitializer extends ChannelInitializer<SocketChannel> {

  private final WebSocketClientHandshaker handShaker;
  private WebsocketProtostuffDecoder decoder;

  public C5ConnectionInitializer(WebSocketClientHandshaker handShaker) {
    super();
    this.handShaker = handShaker;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
   decoder = new WebsocketProtostuffDecoder(handShaker);
   ChannelPipeline pipeline = ch.pipeline();
    pipeline.addLast("logger", new LoggingHandler(LogLevel.DEBUG));
    pipeline.addLast(new HttpClientCodec(), new HttpObjectAggregator(8192));
    pipeline.addLast("websec-codec", new WebsocketProtostuffEncoder(handShaker));
    pipeline.addLast("message-codec", decoder);
    pipeline.addLast("message-handler", new MessageHandler());
  }

  public void syncOnHandshake() throws InterruptedException, TimeoutException, ExecutionException {
      decoder.syncOnHandshake();
  }
}
