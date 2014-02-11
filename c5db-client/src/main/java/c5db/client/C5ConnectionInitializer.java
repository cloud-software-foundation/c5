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
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class C5ConnectionInitializer extends ChannelInitializer<SocketChannel> {

  private final String hostname;
  private final int port;

  C5ConnectionInitializer(String host, int port) {
    super();
    this.hostname = host;
    this.port = port;
  }

  private URI getUri(String host, int port) throws IOException {
    URI uri = null;

    if (host.isEmpty() || port == 0) {
      throw new IOException("Invalid host and/or port provided " +
          "host: " + host + " port: " + port);
    }

    try {
      uri = new URI("ws://" + host + ":" + port + "/websocket");
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    String protocol = uri != null ? uri.getScheme() : null;
    if (!"ws".equals(protocol)) {
      throw new IllegalArgumentException("Unsupported protocol: " + protocol);
    }
    return uri;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    URI uri = getUri(hostname, port);

    HttpHeaders customHeaders = new DefaultHttpHeaders();
    final WebSocketClientHandshaker handShaker =
        WebSocketClientHandshakerFactory.newHandshaker(uri, WebSocketVersion.V13, null, false, customHeaders);

    ChannelPipeline pipeline = ch.pipeline();
    pipeline.addLast("http-codec", new HttpClientCodec());
    pipeline.addLast("aggregator", new HttpObjectAggregator(65536));
    pipeline.addLast("ws-handler", new WebSocketClientProtocolHandler(handShaker));
    pipeline.addLast("websec-codec", new WebsocketProtostuffEncoder());
    pipeline.addLast("message-codec", new WebsocketProtostuffDecoder());
    pipeline.addLast("message-handler", new MessageHandler());

  }
}
