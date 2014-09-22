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
package c5db.control;

import c5db.C5ServerConstants;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.messages.generated.CommandReply;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;

import java.net.InetSocketAddress;

/**
 * A very simple control client that uses 1 shot HTTP requests to ask remote services
 * what to do.  Uses a ListenableFuture interface to convey replies back.
 * <p>
 * It doesn't do any timeouts, or anything of such a nature.
 */
public class SimpleControlClient {
  private final EventLoopGroup ioWorkerGroup;
  private final Bootstrap client = new Bootstrap();

  public SimpleControlClient(EventLoopGroup ioWorkerGroup) {

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

}
