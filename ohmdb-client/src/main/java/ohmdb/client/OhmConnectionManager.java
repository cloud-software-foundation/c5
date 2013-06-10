package ohmdb.client;

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

public enum OhmConnectionManager {
  INSTANCE;

  private final EventLoopGroup group = new NioEventLoopGroup();
  private final ConcurrentHashMap<String, Channel> regionChannelMap =
      new ConcurrentHashMap<>();
  private final Bootstrap bootstrap = new Bootstrap();

  OhmConnectionManager() {
    bootstrap.group(group)
        .channel(NioSocketChannel.class)
        .handler(new RequestInitializer());
  }

  String getHostPortHash(String host, int port) {
    return host + ":" + port;
  }

  public Channel connect(String host, int port)
      throws InterruptedException, IOException {

    if (host.isEmpty() || port == 0) {
      throw new IOException("Invalid host and/or port provided " +
          "host: " + host + " port: " + port);
    } else {
      return bootstrap
          .connect(host, port)
          .sync()
          .channel();
    }
  }

  //TODO thread safe?
  Channel getOrCreateChannel(String host, int port)
      throws IOException, InterruptedException {
    String hash = getHostPortHash(host, port);
    if (!regionChannelMap.containsKey(hash)) {
      regionChannelMap.put(hash, connect(host, port));
    }
    return regionChannelMap.get(hash);
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
