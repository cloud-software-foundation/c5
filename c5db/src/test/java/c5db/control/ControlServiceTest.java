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

import c5db.interfaces.C5Module;
import c5db.interfaces.C5Server;
import c5db.interfaces.DiscoveryModule;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.messages.generated.CommandReply;
import c5db.messages.generated.ModuleSubCommand;
import c5db.messages.generated.ModuleType;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.channels.Request;
import org.jetlang.channels.RequestChannel;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;
import org.jetlang.fibers.ThreadFiber;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Test control service.
 */
public class ControlServiceTest {

  private static final String DONT_CARE_PAYLOAD = "hi there";
  private static final String DONT_CARE_REPLY_PAYLOAD = "yay!";
  private static final String DONT_CARE_EMPTY = "";
  private static final long LOCAL_NODE_ID = 1;
  private static final long NOT_LOCAL_NODE_ID = LOCAL_NODE_ID + 1;
  @Rule
  public JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(new Synchroniser());
  }};

  private final NioEventLoopGroup acceptConnectionGroup = new NioEventLoopGroup(1);
  private final NioEventLoopGroup ioWorkerGroup = new NioEventLoopGroup();
  private final PoolFiberFactory fiberFactory = new PoolFiberFactory(Executors.newFixedThreadPool(2));
  private final C5Server server = context.mock(C5Server.class);
  private final DiscoveryModule discoveryModule = context.mock(DiscoveryModule.class);
  private final SettableFuture<C5Module> getModuleFuture = SettableFuture.create();

  private ControlService controlService ;
  private SimpleControlClient controlClient;

  private int modulePortUnderTest;

  private final RequestChannel<CommandRpcRequest<?>, CommandReply> serverRequests = new MemoryRequestChannel<>();
  private final Fiber ourFiber = new ThreadFiber();

  @Before
  public void before() {
    getModuleFuture.set(discoveryModule);
    Random portRandomizer = new Random();
    modulePortUnderTest = 3000 + portRandomizer.nextInt(3000);

    context.checking(new Expectations() {{
      allowing(server).getCommandRequests();
      will(returnValue(serverRequests));

      allowing(server).getModule(with(equal(ModuleType.Discovery)));
      will(returnValue(getModuleFuture));

      allowing(server).getNodeId();
      will(returnValue(LOCAL_NODE_ID));
    }});

    ourFiber.start();
    serverRequests.subscribe(ourFiber, this::handleServerRequests);


    controlService = new ControlService(
        server,
        fiberFactory.create(),
        acceptConnectionGroup,
        ioWorkerGroup,
        modulePortUnderTest
    );

    controlClient = new SimpleControlClient(ioWorkerGroup);
  }

  private final CommandReply serverReply = new CommandReply(true, DONT_CARE_REPLY_PAYLOAD, DONT_CARE_EMPTY);

  private void handleServerRequests(Request<CommandRpcRequest<?>, CommandReply>  msg) {
    System.out.println("Handle server requests: " + msg.getRequest());

    msg.reply(serverReply);
  }

  @After
  public void after() {
    ourFiber.dispose();

    controlService.stopAndWait();

    acceptConnectionGroup.shutdownGracefully();
    ioWorkerGroup.shutdownGracefully();
  }

  @Test(timeout = 3000)
  public void shouldOpenAHTTPSocketAndAcceptAValidRequest() throws UnknownHostException, InterruptedException, ExecutionException {
    controlService.startAndWait();

    CommandRpcRequest cmd = new CommandRpcRequest<>(LOCAL_NODE_ID, new ModuleSubCommand(ModuleType.Tablet, DONT_CARE_PAYLOAD));
    ListenableFuture<CommandReply> future = controlClient.sendRequest(cmd,
        new InetSocketAddress(InetAddress.getByName("localhost"), modulePortUnderTest));

    CommandReply reply = future.get();
    //assertThat(reply, is(serverReply));
    // TODO make it so that protostuff messages compare via .equals properly
    assertThat(reply.getCommandStdout(), is(serverReply.getCommandStdout()));
    System.out.println("reply is: " + reply);
  }

  @Test(timeout = 3000)
  public void shouldReturnAnErrorWithInvalidNodeId() throws UnknownHostException, ExecutionException, InterruptedException {
    controlService.startAndWait();

    CommandRpcRequest cmd = new CommandRpcRequest<>(NOT_LOCAL_NODE_ID, new ModuleSubCommand(ModuleType.Tablet, DONT_CARE_PAYLOAD));
    ListenableFuture<CommandReply> future = controlClient.sendRequest(cmd,
        new InetSocketAddress(InetAddress.getByName("localhost"), modulePortUnderTest));

    CommandReply reply = future.get();

    assertThat(reply.getCommandSuccess(), is(false));
    System.out.println("reply is: " + reply);
  }
}
