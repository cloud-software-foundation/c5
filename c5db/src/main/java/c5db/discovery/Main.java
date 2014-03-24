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
package c5db.discovery;

import c5db.ConfigDirectory;
import c5db.interfaces.C5Module;
import c5db.interfaces.C5Server;
import c5db.interfaces.DiscoveryModule;
import c5db.messages.generated.CommandReply;
import c5db.messages.generated.ModuleType;
import com.dyuproject.protostuff.Message;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.RequestChannel;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static c5db.discovery.generated.Beacon.Availability;
import static c5db.interfaces.DiscoveryModule.NodeInfo;


public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    final DiscoveryModule beaconService;
    private final String clusterName;
    private final int discoveryPort;
    private final int servicePort;
    private final long nodeId;
    private final Fiber theFiber = new ThreadFiber();
    private final NioEventLoopGroup nioEventLoopGroup = new NioEventLoopGroup();
    private final C5Server theServer;

    Main(String clusterName) throws SocketException, InterruptedException {
        this.clusterName = clusterName;

        // a non-privileged port between 1024 -> ....
        this.discoveryPort = (Math.abs(clusterName.hashCode()) % 16384) + 1024;
        LOG.debug("Cluster port = " + discoveryPort);

        this.servicePort = discoveryPort + (int)(Math.random() * 5000);

        final Availability.Builder builder = Availability.newBuilder();
        builder.setBaseNetworkPort(servicePort);
        nodeId = new Random().nextLong();
        builder.setNodeId(nodeId);

        // nodeId
        // servicePort
        // discoveryPort
        theServer = new C5Server() {
            org.jetlang.channels.Channel<ModuleStateChange> moduleStateChangeChannel = new MemoryChannel<>();
            @Override
            public long getNodeId() {
                return 0;
            }

            @Override
            public ListenableFuture<C5Module> getModule(ModuleType moduleType) {
                return null;
            }

            @Override
            public org.jetlang.channels.Channel<Message<?>> getCommandChannel() {
                return null;
            }

            @Override
            public RequestChannel<Message<?>, CommandReply> getCommandRequests() {
                return null;
            }

            @Override
            public org.jetlang.channels.Channel<ModuleStateChange> getModuleStateChangeChannel() {
                return moduleStateChangeChannel;
            }

            @Override
            public ImmutableMap<ModuleType, C5Module> getModules() throws ExecutionException, InterruptedException {
                return null;
            }

            @Override
            public ListenableFuture<ImmutableMap<ModuleType, C5Module>> getModules2() {
                return null;
            }

            @Override
            public ConfigDirectory getConfigDirectory() {
                return null;
            }

            @Override
            public org.jetlang.channels.Channel<ConfigKeyUpdated> getConfigUpdateChannel() {
                return null;
            }

            @Override
            public ListenableFuture<State> start() {
                return null;
            }

            @Override
            public State startAndWait() {
                return null;
            }

            @Override
            public boolean isRunning() {
                return false;
            }

            @Override
            public State state() {
                return null;
            }

            @Override
            public ListenableFuture<State> stop() {
                return null;
            }

            @Override
            public State stopAndWait() {
                return null;
            }

            @Override
            public Throwable failureCause() {
                return null;
            }

            @Override
            public void addListener(Listener listener, Executor executor) {

            }
        };


        //beaconService = new BeaconService(discoveryPort, builder.buildPartial());
        beaconService = new BeaconService(
                nodeId, discoveryPort, theFiber, nioEventLoopGroup,
                new HashMap<>(),
                theServer
        );
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Specify cluster name as arg1 pls");
            System.exit(1);
        }
        String clusterName = args[0];
        new Main(clusterName).run();
    }

    public void run() throws Exception {
        beaconService.startAndWait();
        LOG.debug("Started");

        // now try to RPC myself a tad:
        final Fiber fiber = new ThreadFiber();
        fiber.start();

        fiber.scheduleAtFixedRate(() -> {
            ListenableFuture<ImmutableMap<Long,NodeInfo>> fut = beaconService.getState();
          try {
            final ImmutableMap<Long,NodeInfo> state = fut.get();
            LOG.debug("State info:");
            for(NodeInfo info : state.values()) {
              LOG.debug(info.toString());
            }
          } catch (InterruptedException | ExecutionException e) {
            LOG.error(e.toString());
          }
        }, 10, 10, TimeUnit.SECONDS);

        final ServerBootstrap b = new ServerBootstrap();
        final NioEventLoopGroup parentGroup = new NioEventLoopGroup(1);
        final NioEventLoopGroup childGroup = new NioEventLoopGroup();
        b.group(parentGroup, childGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 100)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new InboundHandler());
                    }
                });
        final Channel serverChannel = b.bind(servicePort).sync().channel();
        final ImmutableMap<Long,NodeInfo> peers = waitForAPeerOrMore();

        // make a new bootstrap, it's just so silly:
        final Bootstrap b2 = new Bootstrap();
        b2.group(childGroup)
                .option(ChannelOption.TCP_NODELAY, true)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast("stringEncoder", new StringEncoder(CharsetUtil.UTF_8));
                    }
                });
        // we dont even need to do anything else!

        LOG.debug("Listening on module port: " + servicePort);
        // now send messages to all my peers except myself of course, duh.
        for ( NodeInfo peer: peers.values()) {
            if (peer.availability.getNodeId() == nodeId) {
                // yes this is me, and continue
                continue;
            }
            final InetSocketAddress remotePeerAddr = new InetSocketAddress(peer.availability.getAddressesList().get(0),
                    peer.availability.getBaseNetworkPort());
            // uh ok lets connect and make hash:

            final SocketAddress localAddr = serverChannel.localAddress();
            LOG.debug("Writing some junk to: " + remotePeerAddr + " from: " + localAddr);
            final Channel peerChannel = b2.connect(remotePeerAddr, localAddr).channel();
            LOG.debug("Channel RemoteAddr: " + peerChannel.remoteAddress() + " localaddr: " + peerChannel.localAddress());
            // now write a little bit:
            peerChannel.write("42");
        }
    }

    private ImmutableMap<Long,NodeInfo> waitForAPeerOrMore() throws ExecutionException, InterruptedException {
        final Fiber fiber = new ThreadFiber();
        fiber.start();
        final SettableFuture<ImmutableMap<Long,NodeInfo>> future = SettableFuture.create();
        LOG.info("Waiting for peers...");
        return future.get();
    }

}
