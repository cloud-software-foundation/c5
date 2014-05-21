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
package c5db.control;

import c5db.C5ServerConstants;
import c5db.interfaces.C5Server;
import c5db.interfaces.ControlModule;
import c5db.interfaces.DiscoveryModule;
import c5db.interfaces.discovery.NodeInfoReply;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.messages.generated.CommandReply;
import c5db.messages.generated.ModuleType;
import c5db.util.C5Futures;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.protostuff.Message;
import org.jetlang.channels.AsyncRequest;
import org.jetlang.channels.Request;
import org.jetlang.fibers.Fiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Starts a HTTP service to listen for and respond to control messages.
 */
public class ControlService extends AbstractService implements ControlModule {
  private static final Logger LOG = LoggerFactory.getLogger(ControlService.class);

  private final C5Server server;
  private final Fiber serviceFiber;
  private final EventLoopGroup acceptConnectionGroup;
  private final EventLoopGroup ioWorkerGroup;
  private final int modulePort;

  private DiscoveryModule discoveryModule;
  private Channel listenChannel;

  public ControlService(C5Server server,
                        Fiber serviceFiber,
                        EventLoopGroup acceptConnectionGroup,
                        EventLoopGroup ioWorkerGroup,
                        int modulePort) {
    this.server = server;
    this.serviceFiber = serviceFiber;
    this.acceptConnectionGroup = acceptConnectionGroup;
    this.ioWorkerGroup = ioWorkerGroup;
    this.modulePort = modulePort;

    controlClient = new SimpleControlClient(ioWorkerGroup);
  }

  private final SimpleControlClient controlClient;

  @Override
  public void doMessage(Request<CommandRpcRequest<?>, CommandReply> request) {
    ListenableFuture<NodeInfoReply> nodeInfoFuture = discoveryModule.getNodeInfo(request.getRequest().receipientNodeId,
        ModuleType.ControlRpc);
    C5Futures.addCallback(nodeInfoFuture, nodeInfo -> {
      if (nodeInfo.found) {
        String firstAddress = nodeInfo.addresses.get(0);
        int port = nodeInfo.port;
        InetSocketAddress socketAddress = null;
        try {
          socketAddress = new InetSocketAddress(
              InetAddress.getByName(firstAddress), port);

          C5Futures.addCallback(controlClient.sendRequest(request.getRequest(), socketAddress),
              request::reply, exception -> {
                CommandReply reply = new CommandReply(false, "", "Transport error: " + exception);
                request.reply(reply);
              }, serviceFiber
          );
        } catch (UnknownHostException e) {
          LOG.error("Bad address", e);
          CommandReply reply = new CommandReply(false, "", "Bad remote address:" + e);
          request.reply(reply);
        }
      } else {
        CommandReply reply = new CommandReply(false, "", "Bad node id " + request.getRequest().receipientNodeId);
        request.reply(reply);
      }
    }, exception -> {
      LOG.error("Unable to find nodeId! {}", request.getRequest().receipientNodeId);

      CommandReply reply = new CommandReply(false, "", "Unable to find NodeId!");
      request.reply(reply);
    }, serviceFiber);
  }

  @Override
  public ModuleType getModuleType() {
    return ModuleType.ControlRpc;
  }

  @Override
  public boolean hasPort() {
    return true;
  }

  @Override
  public int port() {
    return modulePort;
  }

  @Override
  public String acceptCommand(String commandString) throws InterruptedException {
    return null;
  }

  private class MessageHandler extends SimpleChannelInboundHandler<CommandRpcRequest<? extends Message>> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CommandRpcRequest<? extends Message> msg) throws Exception {
      System.out.println("Server read off: " + msg);
      // this should match our local node id!
      if (msg.receipientNodeId != server.getNodeId()) {
        sendErrorReply(ctx.channel(), new Exception("Bad nodeId!"));
        return;
      }
      AsyncRequest.withOneReply(serviceFiber, server.getCommandRequests(), msg, reply -> {
        // got reply!
        System.out.println("Reply to client: " + reply);
        ctx.channel().writeAndFlush(reply);
        ctx.channel().close();
      }, 1000, TimeUnit.MILLISECONDS, () -> sendErrorReply(ctx.channel(), new Exception("Timed out request")));
    }
  }

  private void sendErrorReply(Channel channel, Exception ex) {
    CommandReply reply = new CommandReply(false, "", ex.toString());
    channel.writeAndFlush(reply);
    channel.close();
  }

  @Override
  protected void doStart() {
    serviceFiber.start();

    serviceFiber.execute(() -> C5Futures.addCallback(server.getModule(ModuleType.Discovery),
        module -> {
          discoveryModule = (DiscoveryModule) module;
          startHttpRpc();
        }, this::notifyFailed, serviceFiber
    ));
  }

  private void startHttpRpc() {
    try {
      ServerBootstrap serverBootstrap = new ServerBootstrap();
      ServerBootstrap serverBootstrap1 = serverBootstrap.group(acceptConnectionGroup, ioWorkerGroup)
          .channel(NioServerSocketChannel.class)
          .option(ChannelOption.SO_REUSEADDR, true)
          .option(ChannelOption.SO_BACKLOG, 100)
          .childOption(ChannelOption.TCP_NODELAY, true)
          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
              ChannelPipeline pipeline = ch.pipeline();

//              pipeline.addLast("logger", new LoggingHandler(LogLevel.DEBUG));
              pipeline.addLast("http-server", new HttpServerCodec());
              pipeline.addLast("aggregator", new HttpObjectAggregator(C5ServerConstants.MAX_CALL_SIZE));


              pipeline.addLast("encode", new ServerHttpProtostuffEncoder());
              pipeline.addLast("decode", new ServerHttpProtostuffDecoder());

              pipeline.addLast("translate", new ServerDecodeCommandRequest());

              pipeline.addLast("inc-messages", new MessageHandler());
            }
          });

      serverBootstrap.bind(modulePort).addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (future.isSuccess()) {
            // yay
            listenChannel = future.channel();
            notifyStarted();
          } else {
            LOG.error("Unable to bind to port {}", modulePort);
            notifyFailed(future.cause());
          }
        }
      });
    } catch (Exception e) {
      notifyFailed(e);
    }
  }

  @Override
  protected void doStop() {
    try {
      listenChannel.close().get();
    } catch (InterruptedException | ExecutionException e) {
      notifyFailed(e);
    }
    notifyStopped();
  }
}
