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

import c5db.codec.UdpProtostuffEncoder;
import c5db.eventLogging.generated.EventLogEntry;
import c5db.interfaces.EventLogModule;
import c5db.messages.generated.ModuleType;
import com.google.common.util.concurrent.AbstractService;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.concurrent.GenericFutureListener;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.Subscriber;
import org.jetlang.fibers.Fiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 *
 */
public class EventLogService extends AbstractService implements EventLogModule {
  private static final Logger LOG = LoggerFactory.getLogger(EventLogService.class);

  private final int port;
  private final Fiber fiber;
  private final NioEventLoopGroup nioEventLoopGroup;
  private final Channel<EventLogEntry> eventLogChannel = new MemoryChannel<>();

  private io.netty.channel.Channel broadcastChannel;
  private final InetSocketAddress sendAddress;

  public EventLogService(int port,
                         Fiber fiber,
                         NioEventLoopGroup nioEventLoopGroup
  ) {
    this.port = port;
    this.fiber = fiber;
    this.nioEventLoopGroup = nioEventLoopGroup;

    sendAddress = new InetSocketAddress("255.255.255.255", port);
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
                p.addLast("protostuffEncoder",
                    new UdpProtostuffEncoder<>(EventLogEntry.getSchema(), false));
              }
            });

        bootstrap.bind(port).addListener(new GenericFutureListener<ChannelFuture>() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            broadcastChannel = future.channel();
          }
        });

        eventLogChannel.subscribe(fiber, msg -> {
          if (broadcastChannel == null) {
            LOG.debug("Broadcast channel isn't read yet, dropped message");
            return;
          }

          LOG.trace("Sending event {}", msg);
          broadcastChannel.writeAndFlush(
              new UdpProtostuffEncoder.UdpProtostuffMessage<>(sendAddress, msg));
        });

        fiber.start();

        notifyStarted();
      } catch (Throwable t) {
        fiber.dispose();

        notifyFailed(t);
      }
    });
  }

  @Override
  protected void doStop() {
    nioEventLoopGroup.next().execute(() -> {
      fiber.dispose();
      if (broadcastChannel != null) {
        broadcastChannel.close();
      }

      notifyStopped();
    });
  }

  @Override
  public Subscriber<EventLogEntry> eventLogChannel() {
    return eventLogChannel;
  }

  @Override
  public ModuleType getModuleType() {
    return ModuleType.EventLog;
  }

  @Override
  public boolean hasPort() {
    return true;
  }

  @Override
  public int port() {
    return port;
  }

  @Override
  public String acceptCommand(String commandString) {
    return null;
  }
}
