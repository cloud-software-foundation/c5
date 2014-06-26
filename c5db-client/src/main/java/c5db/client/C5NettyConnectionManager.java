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

import c5db.client.codec.websocket.Encoder;
import c5db.client.codec.websocket.Initializer;
import c5db.client.generated.Location;
import c5db.client.generated.LocationResponse;
import c5db.client.generated.RegionLocation;
import c5db.client.generated.TableName;
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
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * A class which manages all of the outbound connections from a client to a set of regions/tablets.
 */
public class C5NettyConnectionManager implements C5ConnectionManager {
  private static final Logger LOG = LoggerFactory.getLogger(C5NettyConnectionManager.class);
  private final ConcurrentHashMap<String, Channel> regionChannelMap = new ConcurrentHashMap<>();
  private final Bootstrap bootstrap = new Bootstrap();
  private final EventLoopGroup group = new NioEventLoopGroup();

  public C5NettyConnectionManager() {
    bootstrap.group(group);
    bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
  }

  String getHostPortHash(String host, int port) {
    return host + ":" + port;
  }

  Channel bootStrapConnection(String host, int port)
      throws InterruptedException, TimeoutException, ExecutionException, URISyntaxException {
    URI uri = new URI("ws://" + host + ":" + port + "/websocket");
    final WebSocketClientHandshaker handShaker = WebSocketClientHandshakerFactory.newHandshaker(uri,
        WebSocketVersion.V13,
        null,
        false,
        new DefaultHttpHeaders());
    final Initializer initializer = new Initializer(handShaker);
    bootstrap.channel(NioSocketChannel.class).handler(initializer);
    final ChannelFuture future = bootstrap.connect(host, port);
    final Channel channel = future.sync().channel();
    initializer.syncOnHandshake();
    return channel;
  }

  @Override
  public Channel getBestChannelFor(TableName tableName, byte[] row) {
    return null;
  }

  private boolean isHandShakeConnected(Channel channel) {
    final ChannelPipeline pipeline = channel.pipeline();
    final Encoder encoder = pipeline.get(Encoder.class);
    return encoder.getHandShaker().isHandshakeComplete();
  }

  public void closeChannel(String host, int port) {
    final String hash = getHostPortHash(host, port);
    regionChannelMap.remove(hash);
  }

  @Override
  public void close() throws InterruptedException {
    final List<ChannelFuture> channels = new ArrayList<>();
    for (Channel channel : regionChannelMap.values()) {
      final ChannelFuture channelFuture = channel.close();
      channels.add(channelFuture);
    }

    regionChannelMap.clear();
    for (ChannelFuture future : channels) {
      future.sync();
    }
    group.shutdownGracefully();
  }

  @Override
  public void flush() {
    for (Channel channel : regionChannelMap.values()) {
      channel.flush();
    }
  }

  @Override
  @NotNull
  public Channel getOrCreateChannel(LocationResponse locationResponse)
      throws InterruptedException, ExecutionException, TimeoutException, URISyntaxException {
    List<RegionLocation> regionLocationList = locationResponse.getRegionLocationList();

    if (regionLocationList.size() > 1) {
      LOG.error("we only support using the first location response currently");
    } else if (regionLocationList.size() == 0) {
      throw new ExecutionException(new IOException("invalid location response:" + locationResponse));
    }

    Location location = regionLocationList.get(0).getLocation();
    return getOrCreateChannel(location.getHostname(), location.getPort());
  }

  @Override
  @NotNull
  public Channel getOrCreateChannel(String host, int port)
      throws InterruptedException, ExecutionException, TimeoutException, URISyntaxException {
    final String hash = getHostPortHash(host, port);

    if (!regionChannelMap.containsKey(hash)) {
      final Channel channel = bootStrapConnection(host, port);
      regionChannelMap.put(hash, channel);
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
}
