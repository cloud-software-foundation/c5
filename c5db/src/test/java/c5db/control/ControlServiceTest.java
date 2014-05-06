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

import c5db.interfaces.C5Server;
import c5db.interfaces.DiscoveryModule;
import c5db.interfaces.discovery.NodeInfoReply;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.messages.generated.CommandReply;
import c5db.messages.generated.ModuleSubCommand;
import c5db.messages.generated.ModuleType;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.channels.Request;
import org.jetlang.channels.RequestChannel;
import org.jetlang.channels.Session;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;
import org.jetlang.fibers.ThreadFiber;
import org.jmock.Expectations;
import org.jmock.api.Action;
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

import static c5db.FutureActions.returnFutureWithException;
import static c5db.FutureActions.returnFutureWithValue;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Test control service.
 */
public class ControlServiceTest {

  private static final String DO_NOT_CARE_PAYLOAD = "hi there";
  private static final String DO_NOT_CARE_REPLY_PAYLOAD = "yay!";
  private static final String DO_NOT_CARE_EMPTY = "";
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

  private ControlService controlService ;
  private SimpleControlClient controlClient;

  private int modulePortUnderTest;

  private final RequestChannel<CommandRpcRequest<?>, CommandReply> serverRequests = new MemoryRequestChannel<>();
  private final Fiber ourFiber = new ThreadFiber();

  @Before
  public void before() {
    Random portRandomizer = new Random();
    modulePortUnderTest = 3000 + portRandomizer.nextInt(3000);

    context.checking(new Expectations() {{
      allowing(server).getCommandRequests();
      will(returnValue(serverRequests));

      allowing(server).getModule(with(equal(ModuleType.Discovery)));
      will(returnFutureWithValue(discoveryModule));

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

    controlService.startAndWait();
  }

  private final CommandReply serverReply = new CommandReply(true, DO_NOT_CARE_REPLY_PAYLOAD, DO_NOT_CARE_EMPTY);

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
    CommandReply reply = controlClient.sendRequest(rpcRequest(), moduleInetAddress()).get();
    //assertThat(reply, is(serverReply));
    // TODO make it so that protostuff messages compare via .equals properly
    assertThat(reply.getCommandStdout(), is(serverReply.getCommandStdout()));
    System.out.println("reply is: " + reply);
  }

  @Test(timeout = 3000)
  public void shouldReturnAnErrorWithInvalidNodeId() throws UnknownHostException, ExecutionException, InterruptedException {
    CommandReply reply = controlClient.sendRequest(rpcRequestWithWrongNodeId(), moduleInetAddress()).get();

    assertThat(reply.getCommandSuccess(), is(false));
    System.out.println("reply is: " + reply);
  }

  private InetSocketAddress moduleInetAddress() throws UnknownHostException {
    return new InetSocketAddress(InetAddress.getByName("localhost"), modulePortUnderTest);
  }

  @Test(timeout = 3000)
  public void shouldHaveEmbededClientCalledByDoMessageCallItself() throws ExecutionException, InterruptedException {
    context.checking(new Expectations() {{
      allowing(discoveryModule).getNodeInfo(LOCAL_NODE_ID, ModuleType.ControlRpc);
      will(returnFutureWithNodeInfo(nodeInfo().withPort(modulePortUnderTest)));
    }});

    ReplyWaiter<CommandRpcRequest<?>, CommandReply> waiter = new ReplyWaiter<>(rpcRequest());

    controlService.doMessage(waiter);

    CommandReply reply = waiter.repliedFuture.get();

    // TODO better compare
    assertThat(reply.getCommandSuccess(), is(true));
    assertThat(reply.getCommandStdout(), is(serverReply.getCommandStdout()));
  }

  @Test(timeout = 3000)
  public void embededClientHandlesCantFindNode() throws ExecutionException, InterruptedException {
    context.checking(new Expectations() {{
      allowing(discoveryModule).getNodeInfo(LOCAL_NODE_ID, ModuleType.ControlRpc);
      will(returnFutureWithNodeInfo(nodeInfo().failedToLookupNode()));
    }});
    ReplyWaiter<CommandRpcRequest<?>, CommandReply> waiter = new ReplyWaiter<>(rpcRequest());

    controlService.doMessage(waiter);

    CommandReply reply = waiter.repliedFuture.get();

    assertThat(reply.getCommandSuccess(), is(false));
    System.out.println(reply);
  }

  @Test(timeout = 3000)
  public void embeddedClientBadDNSLoopup() throws ExecutionException, InterruptedException {
    context.checking(new Expectations(){{
      allowing(discoveryModule).getNodeInfo(LOCAL_NODE_ID, ModuleType.ControlRpc);
      will(returnFutureWithNodeInfo(nodeInfo().withAddress("WEIRD_ADDRESS_DNS_FAIL_ME")));
    }});
    ReplyWaiter<CommandRpcRequest<?>, CommandReply> waiter = new ReplyWaiter<>(rpcRequest());

    controlService.doMessage(waiter);

    CommandReply reply = waiter.repliedFuture.get();

    assertThat(reply.getCommandSuccess(), is(false));
    System.out.println(reply);
  }

  @Test(timeout = 3000)
  public void embeddedClientWasGivenAnIncorrectPort() throws ExecutionException, InterruptedException {
    context.checking(new Expectations() {{
      allowing(discoveryModule).getNodeInfo(LOCAL_NODE_ID, ModuleType.ControlRpc);
      will(returnFutureWithNodeInfo(nodeInfo().withPort(modulePortUnderTest + 1)));
    }});
    ReplyWaiter<CommandRpcRequest<?>, CommandReply> waiter = new ReplyWaiter<>(rpcRequest());

    controlService.doMessage(waiter);

    CommandReply reply = waiter.repliedFuture.get();

    assertThat(reply.getCommandSuccess(), is(false));
    System.out.println(reply);
  }

  @Test(timeout = 3000)
  public void embeddedClientGotExceptionFromDiscoveryModule() throws ExecutionException, InterruptedException {
    context.checking(new Expectations() {{
      allowing(discoveryModule).getNodeInfo(LOCAL_NODE_ID, ModuleType.ControlRpc);
      will(returnFutureWithException(new Exception()));
    }});
    ReplyWaiter<CommandRpcRequest<?>, CommandReply> waiter = new ReplyWaiter<>(rpcRequest());

    controlService.doMessage(waiter);

    CommandReply reply = waiter.repliedFuture.get();

    assertThat(reply.getCommandSuccess(), is(false));
    System.out.println(reply);
  }

  @Test
  public void fudgeCodeCoverage() throws InterruptedException {
    assertThat(controlService.getModuleType(), is(ModuleType.ControlRpc));
    assertThat(controlService.hasPort(), is(true));
    assertThat(controlService.port(), is(modulePortUnderTest));
    assertThat(controlService.acceptCommand(null), is(nullValue()));
  }

  private static ModuleSubCommand subCommand() {
    return new ModuleSubCommand(ModuleType.Tablet, DO_NOT_CARE_PAYLOAD);
  }

  private static CommandRpcRequest<ModuleSubCommand> rpcRequest() {
    return new CommandRpcRequest<>(LOCAL_NODE_ID, subCommand());
  }

  private static CommandRpcRequest<ModuleSubCommand> rpcRequestWithWrongNodeId() {
    return new CommandRpcRequest<>(NOT_LOCAL_NODE_ID, subCommand());
  }

  public static class ReplyWaiter<R, V> implements Request<R, V> {

    private final R requestValue;
    public final SettableFuture<V> repliedFuture = SettableFuture.create();

    public ReplyWaiter(R requestValue) {

      this.requestValue = requestValue;
    }

    @Override
    public Session getSession() {
      return null;
    }

    @Override
    public R getRequest() {
      return requestValue;
    }

    @Override
    public void reply(V repliedMessage) {
      repliedFuture.set(repliedMessage);
    }
  }

  private static NodeInfoReplyBuilder nodeInfo() {
    return new NodeInfoReplyBuilder();
  }

  public static class NodeInfoReplyBuilder {
    private boolean successFlag = true;
    private String address = "localhost";
    private int port;

    public NodeInfoReplyBuilder withPort(int port) {
      this.port = port;
      return this;
    }

    public NodeInfoReplyBuilder withAddress(String address) {
      this.address = address;
      return this;
    }

    public NodeInfoReplyBuilder failedToLookupNode() {
      successFlag = false;
      return this;
    }

    NodeInfoReply build() {
      return new NodeInfoReply(successFlag, Lists.newArrayList(address), port);
    }
  }
  public static Action returnFutureWithNodeInfo(NodeInfoReplyBuilder builder) {
    return returnFutureWithValue(builder.build());
  }
}
