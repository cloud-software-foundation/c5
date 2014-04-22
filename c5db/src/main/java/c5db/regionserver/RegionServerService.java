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
import c5db.messages.generated.ModuleType;
import c5db.util.C5FiberFactory;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetlang.fibers.Fiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The service handler for the RegionServer class. Responsible for handling the internal lifecycle
 * and attaching the netty infrastructure to the region server.
 */
public class RegionServerService extends AbstractService implements RegionServerModule {
  private static final Logger LOG = LoggerFactory.getLogger(RegionServerService.class);

  private final C5FiberFactory fiberFactory;
  private final Fiber fiber;
  private final NioEventLoopGroup acceptGroup;
  private final NioEventLoopGroup workerGroup;
  private final int port;
  private final C5Server server;
  private final ServerBootstrap bootstrap = new ServerBootstrap();


  TabletModule tabletModule;

  public RegionServerService(NioEventLoopGroup acceptGroup,
                             NioEventLoopGroup workerGroup,
                             int port,
                             C5Server server) {
    this.acceptGroup = acceptGroup;
    this.workerGroup = workerGroup;
    this.port = port;
    this.server = server;
    this.fiberFactory = server.getFiberFactory(this::notifyFailed);

    this.fiber = fiberFactory.create();
  }

  @Override
  protected void doStart() {
    fiber.start();

    fiber.execute(() -> {
      // we need the tablet module:
      ListenableFuture<C5Module> f = server.getModule(ModuleType.Tablet);
      Futures.addCallback(f, new FutureCallback<C5Module>() {
        @Override
        public void onSuccess(C5Module result) {
          tabletModule = (TabletModule) result;
          bootstrap.group(acceptGroup, workerGroup)
              .option(ChannelOption.SO_REUSEADDR, true)
              .childOption(ChannelOption.TCP_NODELAY, true)
              .channel(NioServerSocketChannel.class)
              .childHandler(new ChannelInitializer<SocketChannel>() {
                              @Override
                              protected void initChannel(SocketChannel ch) throws Exception {
                                ChannelPipeline p = ch.pipeline();
                                p.addLast("logger", new LoggingHandler(LogLevel.DEBUG));
                                p.addLast("http-server-codec", new HttpServerCodec());
                                p.addLast("http-agg", new HttpObjectAggregator(C5ServerConstants.MAX_CALL_SIZE));
                                p.addLast("websocket-agg", new WebSocketFrameAggregator(C5ServerConstants.MAX_CALL_SIZE));
                                p.addLast("decoder", new WebsocketProtostuffDecoder("/websocket"));
                                p.addLast("encoder", new WebsocketProtostuffEncoder());
                                p.addLast("handler", new C5ServerHandler(RegionServerService.this));
                              }
                            }
              );

          bootstrap.bind(port).addListener(future -> {
            if (future.isSuccess()) {
              notifyStarted();
            } else {
              LOG.error("Unable to find Region Server to {} {}", port, future.cause());
              notifyFailed(future.cause());
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

  public HRegion getOnlineRegion(RegionSpecifier regionSpecifier) {
    String stringifiedRegion = Bytes.toString(regionSpecifier.getValue().array());
    LOG.debug("get online region:" + stringifiedRegion);
    return tabletModule.getTablet(stringifiedRegion);
  }
}
