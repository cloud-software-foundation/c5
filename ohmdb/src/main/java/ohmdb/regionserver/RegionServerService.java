/*
 * Copyright (C) 2013  Ohm Data
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
package ohmdb.regionserver;

import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import ohmdb.client.generated.ClientProtos;
import ohmdb.interfaces.OhmModule;
import ohmdb.interfaces.OhmServer;
import ohmdb.interfaces.RegionServerModule;
import ohmdb.interfaces.TabletModule;
import ohmdb.messages.generated.ControlMessages;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class RegionServerService extends AbstractService implements RegionServerModule {
    private static final Logger LOG = LoggerFactory.getLogger(RegionServerService.class);

    private final PoolFiberFactory fiberFactory;
    private final Fiber fiber;
    private final NioEventLoopGroup acceptGroup;
    private final NioEventLoopGroup workerGroup;
    private final int port;
    private final OhmServer server;
    private final ServerBootstrap bootstrap = new ServerBootstrap();

    TabletModule tabletModule;

    public RegionServerService(PoolFiberFactory fiberFactory,
                               NioEventLoopGroup acceptGroup,
                               NioEventLoopGroup workerGroup,
                               int port,
                               OhmServer server) {
        this.fiberFactory = fiberFactory;
        this.acceptGroup = acceptGroup;
        this.workerGroup = workerGroup;
        this.port = port;
        this.server = server;

        this.fiber = fiberFactory.create();
    }

    @Override
    protected void doStart() {
        fiber.start();

        fiber.execute(new Runnable() {
            @Override
            public void run() {
                // we need the tablet module:
                ListenableFuture<OhmModule> f = server.getModule(ControlMessages.ModuleType.Tablet);
                Futures.addCallback(f, new FutureCallback<OhmModule>() {
                    @Override
                    public void onSuccess(OhmModule result) {
                        tabletModule = (TabletModule) result;

                        bootstrap.group(acceptGroup, workerGroup)
                                .option(ChannelOption.SO_REUSEADDR, true)
                                .childOption(ChannelOption.TCP_NODELAY, true)
                                .channel(NioServerSocketChannel.class)
                                .childHandler(
                                        new ChannelInitializer<SocketChannel>() {
                                            @Override
                                            protected void initChannel(SocketChannel ch) throws Exception {
                                                ChannelPipeline p = ch.pipeline();

                                                p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
                                                p.addLast("protobufDecoder",
                                                        new ProtobufDecoder(ClientProtos.Call.getDefaultInstance()));

                                                p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
                                                p.addLast("protobufEncoder", new ProtobufEncoder());

                                                p.addLast("handler", new OhmServerHandler(RegionServerService.this));
                                            }
                                        }
                                );

                        bootstrap.bind(port).addListener(new ChannelFutureListener() {
                            @Override
                            public void operationComplete(ChannelFuture future) throws Exception {
                                if (future.isSuccess()) {
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
            }
        });
    }

    @Override
    protected void doStop() {

    }

    @Override
    public ControlMessages.ModuleType getModuleType() {
        return ControlMessages.ModuleType.RegionServer;
    }

    @Override
    public boolean hasPort() {
        return true;
    }

    @Override
    public int port() {
        return port;
    }

    public HRegion getOnlineRegion(String regionName) {
        // TODO note that regionName is just a placeholder, the following call only returns a single tablet every time.
        return tabletModule.getTablet(regionName);
    }
}
