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

import c5db.client.codec.WebsocketProtostuffEncoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import org.mortbay.log.Log;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class C5ConnectionManager {

  RegionChannelMap regionChannelMap = RegionChannelMap.INSTANCE;
  private Bootstrap bootstrap = new Bootstrap();
  private EventLoopGroup group = new NioEventLoopGroup();
  URI uri = null;

  public C5ConnectionManager() {
    bootstrap.group(group);


    try {
      uri = new URI("ws://0.0.0.0:8080/websocket");
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }

  }

  String getHostPortHash(String host, int port) {
    return host + ":" + port;
  }

  public Channel connect(String host, int port) throws InterruptedException, IOException, TimeoutException, ExecutionException {
    WebSocketClientHandshaker handShaker = WebSocketClientHandshakerFactory.newHandshaker(
        uri, WebSocketVersion.V13, null, false, new DefaultHttpHeaders());
    C5ConnectionInitializer initializer = new C5ConnectionInitializer(handShaker);
    bootstrap.channel(NioSocketChannel.class).handler(initializer);

    ChannelFuture future = bootstrap.connect(host, port);
    Channel channel = future.sync().channel();
    initializer.syncOnHandshake();
    return channel;
  }

  public Channel getOrCreateChannel(String host, int port)
      throws IOException, InterruptedException, TimeoutException, ExecutionException {
    String hash = getHostPortHash(host, port);
    Channel channel;
    if (!regionChannelMap.containsKey(hash)) {
      channel = connect(host, port);
      regionChannelMap.put(hash, channel);
      Log.warn("Channel" + channel);
      return channel;
    }

    channel = regionChannelMap.get(hash);

    // Clear stale channels
    if (!(channel.isOpen() && channel.isActive() && isHandShakeConnected(channel))) {
      closeChannel(host, port);
      channel.disconnect();

      channel = getOrCreateChannel(host, port);
    }
    return channel;
  }

  private boolean isHandShakeConnected(Channel channel) {
    ChannelPipeline p = channel.pipeline();
    WebsocketProtostuffEncoder encoder = p.get(WebsocketProtostuffEncoder.class);
    return encoder.handShaker.isHandshakeComplete();
  }

  public void closeChannel(String host, int port)
      throws InterruptedException, ExecutionException, TimeoutException {
    String hash = getHostPortHash(host, port);
    regionChannelMap.remove(hash);
  }

  public void close() throws InterruptedException {
    List<ChannelFuture> channels = new ArrayList<>();
    for (Channel channel : regionChannelMap.getValues()) {
      ChannelFuture channelFuture = channel.close();
      channels.add(channelFuture);
    }

    regionChannelMap.clear();
    for (ChannelFuture future : channels) {
      future.sync();
    }
    group.shutdownGracefully();
  }
}
