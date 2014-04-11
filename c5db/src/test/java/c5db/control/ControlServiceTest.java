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
import io.netty.bootstrap.Bootstrap;
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
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.jetlang.fibers.PoolFiberFactory;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.concurrent.Executors;

/**
 * Test control service.
 */
public class ControlServiceTest {

  @Rule
  public JUnitRuleMockery context = new JUnitRuleMockery();

  private final NioEventLoopGroup acceptConnectionGroup = new NioEventLoopGroup(1);
  private final NioEventLoopGroup ioWorkerGroup = new NioEventLoopGroup();
  private final PoolFiberFactory fiberFactory = new PoolFiberFactory(Executors.newFixedThreadPool(2));

  private ControlService controlService ;
  private int modulePortUnderTest;

  private final Bootstrap client = new Bootstrap();


  @Before
  public void before() {
    Random portRandomizer = new Random();
    modulePortUnderTest = 3000 + portRandomizer.nextInt(3000);

    controlService = new ControlService(
        fiberFactory.create(),
        acceptConnectionGroup,
        ioWorkerGroup,
        modulePortUnderTest
    );

    client.group(ioWorkerGroup)
        .channel(NioSocketChannel.class)
        .option(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.TCP_NODELAY, true)
        .handler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast("logger", new LoggingHandler(LogLevel.DEBUG));
            pipeline.addLast("http-client", new HttpClientCodec());
            pipeline.addLast("aggregator", new HttpObjectAggregator(C5ServerConstants.MAX_CALL_SIZE));

            pipeline.addLast("encode", new ClientHttpProtostuffEncoder());
            pipeline.addLast("decode", new ClientHttpProtostuffDecoder());

            pipeline.addLast(new MessageHandler());
          }
        });
  }

  @After
  public void after() {
    ioWorkerGroup.shutdownGracefully();
  }

  @Test(timeout = 3000)
  public void shouldOpenAHTTPSocketAndAcceptAConnection() throws UnknownHostException, InterruptedException {

    controlService.startAndWait();

    client.connect(InetAddress.getByName("localhost"), modulePortUnderTest).sync();
  }

  private class MessageHandler extends SimpleChannelInboundHandler<Object> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
      System.out.println("Got message: " + msg);
    }
  }
}
