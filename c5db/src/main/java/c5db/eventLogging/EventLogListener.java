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
