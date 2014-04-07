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

package c5db.eventLogging;

import c5db.codec.UdpProtostuffDecoder;
import c5db.eventLogging.generated.EventLogEntry;
import com.google.common.util.concurrent.AbstractService;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ryan on 1/30/14.
 */
public class EventLogListener extends AbstractService {
  private static final Logger LOG = LoggerFactory.getLogger(EventLogListener.class);

  private final int port;
  private final NioEventLoopGroup nioEventLoopGroup;

  private Channel channel;

  public EventLogListener(int port, NioEventLoopGroup nioEventLoopGroup) {
    this.port = port;
    this.nioEventLoopGroup = nioEventLoopGroup;
  }

  private class MsgHandler extends SimpleChannelInboundHandler<EventLogEntry> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, EventLogEntry msg) throws Exception {
      LOG.info("Event: {}", msg);
    }
  }

  @Override
  protected void doStart() {
    nioEventLoopGroup.next().execute(() -> {

      Bootstrap bootstrap = new Bootstrap();
      try {
        bootstrap.group(nioEventLoopGroup)
            .channel(NioDatagramChannel.class)
            .option(ChannelOption.SO_BROADCAST, true)
            .option(ChannelOption.SO_REUSEADDR, true)
            .handler(new ChannelInitializer<DatagramChannel>() {
              @Override
              protected void initChannel(DatagramChannel ch) throws Exception {
                ChannelPipeline p = ch.pipeline();
                p.addLast("protostuffDecoder",
                    new UdpProtostuffDecoder<>(EventLogEntry.getSchema(), false));
                p.addLast("logger",
                    new MsgHandler());
              }
            });

        bootstrap.bind(port).addListener(new GenericFutureListener<ChannelFuture>() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            channel = future.channel();
          }
        });


        notifyStarted();
      } catch (Throwable t) {
        notifyFailed(t);
      }
    });
  }

  @Override
  protected void doStop() {
    nioEventLoopGroup.next().execute(() -> {
      if (channel != null) {
        channel.close();
      }

      notifyStopped();
    });
  }
}
