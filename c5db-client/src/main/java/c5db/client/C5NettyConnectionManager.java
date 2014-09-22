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

import c5db.client.codec.WebsocketProtostuffEncoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import org.mortbay.log.Log;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * A class which manages all of the outbound connections from a client to a set of regions/tablets.
 */
public class C5NettyConnectionManager implements C5ConnectionManager {
  private final RegionChannelMap regionChannelMap = RegionChannelMap.INSTANCE;
  private final Bootstrap bootstrap = new Bootstrap();

  private final EventLoopGroup group = new NioEventLoopGroup();
  private URI uri;

  public C5NettyConnectionManager() {
    bootstrap.group(group);
    bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

    try {
      uri = new URI("ws://0.0.0.0:8080/websocket");
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }

  }

  String getHostPortHash(String host, int port) {
    return host + ":" + port;
  }

  Channel connect(String host, int port)
      throws InterruptedException, TimeoutException, ExecutionException {
    final WebSocketClientHandshaker handShaker = WebSocketClientHandshakerFactory.newHandshaker(uri,
        WebSocketVersion.V13,
        null,
        false,
        new DefaultHttpHeaders());
    final C5ConnectionInitializer initializer = new C5ConnectionInitializer(handShaker);
    bootstrap.channel(NioSocketChannel.class).handler(initializer);

    final ChannelFuture future = bootstrap.connect(host, port);
    final Channel channel = future.sync().channel();
    initializer.syncOnHandshake();

    return channel;
  }

  @Override
  public Channel getOrCreateChannel(byte tableName, byte row) {
    // look up the leader in the local meta cache

    return null;
  }

  @Override
  public Channel getOrCreateChannel(String host, int port)
      throws InterruptedException, ExecutionException, TimeoutException {
    final String hash = getHostPortHash(host, port);
    if (!regionChannelMap.containsKey(hash)) {
      final Channel channel = connect(host, port);
      regionChannelMap.put(hash, channel);
      Log.warn("Channel" + channel);
      return channel;
    }

    Channel channel = regionChannelMap.get(hash);

    // Clear stale channels
    if (!(channel.isOpen() && channel.isActive() && isHandShakeConnected(channel))) {
      closeChannel(host, port);
      channel.disconnect();
      channel = getOrCreateChannel(host, port);
    }

    return channel;
  }

  private boolean isHandShakeConnected(Channel channel) {
    final ChannelPipeline pipeline = channel.pipeline();
    final WebsocketProtostuffEncoder encoder = pipeline.get(WebsocketProtostuffEncoder.class);
    return encoder.getHandShaker().isHandshakeComplete();
  }

  @Override
  public void closeChannel(String host, int port) {
    final String hash = getHostPortHash(host, port);
    regionChannelMap.remove(hash);
  }

  @Override
  public void close() throws InterruptedException {
    final List<ChannelFuture> channels = new ArrayList<>();
    for (Channel channel : regionChannelMap.getValues()) {
      final ChannelFuture channelFuture = channel.close();
      channels.add(channelFuture);
    }

    regionChannelMap.clear();
    for (ChannelFuture future : channels) {
      future.sync();
    }
    group.shutdownGracefully();
  }
}
