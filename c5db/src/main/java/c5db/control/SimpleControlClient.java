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
import c5db.interfaces.DiscoveryModule;
import c5db.interfaces.discovery.NodeInfoReply;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.messages.generated.CommandReply;
import c5db.messages.generated.ModuleType;
import c5db.util.C5Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * A very simple control client that uses 1 shot HTTP requests to ask remote services
 * what to do.  Uses a ListenableFuture interface to convey replies back.
 * <p>
 * It doesn't do any timeouts, or anything of such a nature.
 */
public class SimpleControlClient {
  private final NioEventLoopGroup ioWorkerGroup;
  private final Bootstrap client = new Bootstrap();

  public SimpleControlClient(NioEventLoopGroup ioWorkerGroup) {

    this.ioWorkerGroup = ioWorkerGroup;

    createClient();
  }

  private void createClient() {
    client.group(ioWorkerGroup)
        .channel(NioSocketChannel.class)
        .option(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.TCP_NODELAY, true)
        .handler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
//            pipeline.addLast("logger", new LoggingHandler(LogLevel.WARN));
            pipeline.addLast("http-client", new HttpClientCodec());
            pipeline.addLast("aggregator", new HttpObjectAggregator(C5ServerConstants.MAX_CALL_SIZE));

            pipeline.addLast("encode", new ClientHttpProtostuffEncoder());
            pipeline.addLast("decode", new ClientHttpProtostuffDecoder());

            pipeline.addLast("translate", new ClientEncodeCommandRequest());
          }
        });
  }

  public ListenableFuture<CommandReply> sendRequest(CommandRpcRequest<?> request,
                                                    InetSocketAddress remoteAddress) {
    SettableFuture<CommandReply> replyMessageFuture = SettableFuture.create();
    ChannelFuture connectFuture = client.connect(remoteAddress);
    connectFuture.addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        if (future.isSuccess()) {
          future.channel().pipeline().addLast(new SimpleChannelInboundHandler<CommandReply>() {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, CommandReply msg) throws Exception {
              replyMessageFuture.set(msg);
              ctx.channel().close();
            }
          });

          // connected is fine, flush message:
          future.channel().writeAndFlush(request);
        } else {
          replyMessageFuture.setException(future.cause());
          future.channel().close();
        }
      }
    });

    return replyMessageFuture;
  }
  public ListenableFuture<CommandReply> sendRequest(CommandRpcRequest<?> request, DiscoveryModule discoveryModule) {
    ListenableFuture<NodeInfoReply> nodeInfoFuture = discoveryModule.getNodeInfo(request.receipientNodeId,
        ModuleType.ControlRpc);
    C5Futures.addCallback(nodeInfoFuture, nodeInfo -> {


      /*if (nodeInfo.found) {
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
*/
  }
}
