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
