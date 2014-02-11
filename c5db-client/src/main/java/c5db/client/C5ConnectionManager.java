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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public enum C5ConnectionManager {
  INSTANCE;

  private final EventLoopGroup group = new NioEventLoopGroup();
  private final ConcurrentHashMap<String, Channel> regionChannelMap =
      new ConcurrentHashMap<>();
  private final Bootstrap bootstrap = new Bootstrap();

  String getHostPortHash(String host, int port) {
    return host + ":" + port;
  }

  public Channel connect(String host, int port)
      throws InterruptedException, IOException {
    bootstrap.group(group).channel(NioSocketChannel.class).handler(new C5ConnectionInitializer(host, port));
    ChannelFuture future = bootstrap.connect(host, port);
    return future.sync().channel();
  }
  //TODO thread safe?
  public Channel getOrCreateChannel(String host, int port)
      throws IOException, InterruptedException {
    String hash = getHostPortHash(host, port);
    if (!regionChannelMap.containsKey(hash)) {
      Channel channel = connect(host, port);
      regionChannelMap.put(hash, channel);
    }
    return regionChannelMap.get(hash);
  }

  public ChannelFuture closeChannel(String host, int port) {
    String hash = getHostPortHash(host, port);

    Channel channel = regionChannelMap.get(hash);
    return channel.close();
  }

  public void close() throws InterruptedException {
    List<ChannelFuture> channels = new ArrayList<>();

    for (Channel channel : regionChannelMap.values()) {
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
