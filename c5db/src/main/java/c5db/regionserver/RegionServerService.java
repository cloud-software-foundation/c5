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

package c5db.regionserver;

import c5db.C5ServerConstants;
import c5db.client.generated.RegionSpecifier;
import c5db.codec.WebsocketProtostuffDecoder;
import c5db.codec.WebsocketProtostuffEncoder;
import c5db.interfaces.C5Module;
import c5db.interfaces.C5Server;
import c5db.interfaces.RegionServerModule;
import c5db.interfaces.TabletModule;
import c5db.interfaces.tablet.Tablet;
import c5db.messages.generated.ModuleType;
import c5db.tablet.Region;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetlang.fibers.Fiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

/**
 * The service handler for the RegionServer class. Responsible for handling the internal lifecycle
 * and attaching the netty infrastructure to the region server.
 */
public class RegionServerService extends AbstractService implements RegionServerModule {
  private static final Logger LOG = LoggerFactory.getLogger(RegionServerService.class);

  private final Fiber fiber;
  private final EventLoopGroup acceptGroup;
  private final EventLoopGroup workerGroup;
  private final int port;
  private final C5Server server;
  private final ServerBootstrap bootstrap = new ServerBootstrap();
  private TabletModule tabletModule;
  private Channel listenChannel;

  public RegionServerService(EventLoopGroup acceptGroup,
                             EventLoopGroup workerGroup,
                             int port,
                             C5Server server) {
    this.acceptGroup = acceptGroup;
    this.workerGroup = workerGroup;
    this.port = port;
    this.server = server;
    this.fiber = server.getFiberSupplier().getFiber(this::notifyFailed);
    bootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
  }

  @Override
  protected void doStart() {
    fiber.start();

    fiber.execute(() -> {
      // we need the tablet module:
      ListenableFuture<C5Module> f = server.getModule(ModuleType.Tablet);
      Futures.addCallback(f, new FutureCallback<C5Module>() {
        @Override
        public void onSuccess(final C5Module result) {
          tabletModule = (TabletModule) result;
          bootstrap.group(acceptGroup, workerGroup)
              .option(ChannelOption.SO_REUSEADDR, true)
              .childOption(ChannelOption.TCP_NODELAY, true)
              .channel(NioServerSocketChannel.class)
              .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                  ChannelPipeline p = ch.pipeline();
                  p.addLast("http-server-codec", new HttpServerCodec());
                  p.addLast("http-agg", new HttpObjectAggregator(C5ServerConstants.MAX_CALL_SIZE));
                  p.addLast("websocket-agg", new WebSocketFrameAggregator(C5ServerConstants.MAX_CALL_SIZE));
                  p.addLast("decoder", new WebsocketProtostuffDecoder("/websocket"));
                  p.addLast("encoder", new WebsocketProtostuffEncoder());
                  p.addLast("handler", new RegionServerHandler(RegionServerService.this));
                }
              }
              );

          bootstrap.bind(port).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
              if (future.isSuccess()) {
                listenChannel = future.channel();
                notifyStarted();
              } else {
                LOG.error("Unable to find Region Server to {} {}", port, future.cause());
                notifyFailed(future.cause());
              }
            }
          });
        }

        @Override
        public void onFailure(Throwable t) {
          notifyFailed(t);
        }
      }, fiber);
    });
  }

  @Override
  protected void doStop() {
    try {
      listenChannel.close().get();
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
      notifyFailed(e);
    }

    notifyStopped();
  }

  @Override
  public ModuleType getModuleType() {
    return ModuleType.RegionServer;
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

  public Region getOnlineRegion(RegionSpecifier regionSpecifier) throws RegionNotFoundException {
    ByteBuffer regionSpecifierBuffer = regionSpecifier.getValue();
    if (regionSpecifierBuffer == null) {
      throw new RegionNotFoundException("No region specifier specified in the request");
    }

    String stringifiedRegion = Bytes.toString(regionSpecifierBuffer.array());
    LOG.debug("get online region:" + stringifiedRegion);

    Tablet tablet = tabletModule.getTablet(stringifiedRegion);
    if (tablet == null) {
      throw new RegionNotFoundException("Unable to find specified tablet:" + stringifiedRegion);
    }
    return tablet.getRegion();
  }

  public String toString() {

    return super.toString() + '{' + "port = " + port + '}';
  }

}
