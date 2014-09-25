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

package c5db.tablet;

import c5db.C5ServerConstants;
import c5db.CommandMatchers;
import c5db.client.generated.Cell;
import c5db.client.generated.Condition;
import c5db.client.generated.MutationProto;
import c5db.client.generated.Scan;
import c5db.control.ControlService;
import c5db.interfaces.C5Server;
import c5db.interfaces.ControlModule;
import c5db.interfaces.DiscoveryModule;
import c5db.interfaces.discovery.NodeInfoReply;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.interfaces.tablet.Tablet;
import c5db.messages.generated.CommandReply;
import c5db.messages.generated.ModuleType;
import c5db.regionserver.AddElementsActionReturnTrue;
import c5db.tablet.tabletCreationBehaviors.RootTabletLeaderBehavior;
import com.google.common.collect.Lists;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.core.RunnableExecutorImpl;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static c5db.AsyncChannelAsserts.ChannelListener;
import static c5db.AsyncChannelAsserts.assertEventually;
import static c5db.AsyncChannelAsserts.waitForReply;
import static c5db.FutureActions.returnFutureWithValue;

public class RootTabletLeaderBehaviorTest {

  @Rule
  public final JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(new Synchroniser());
  }};

  private final c5db.interfaces.tablet.Tablet hRegionTablet = context.mock(Tablet.class, "mockHRegionTablet");
  private final Region region = context.mock(Region.class, "mockRegion");
  private final C5Server c5Server = context.mock(C5Server.class, "mockC5Server");
  private final DiscoveryModule discoveryModule = context.mock(DiscoveryModule.class);
  private final RegionScanner regionScanner = context.mock(RegionScanner.class);
  private final Fiber serviceFiber =
      new ThreadFiber(new RunnableExecutorImpl(), "root-leader-behavior-test-fiber", false);
  private final NioEventLoopGroup acceptConnectionGroup = new NioEventLoopGroup(1);
  private final NioEventLoopGroup ioWorkerGroup = new NioEventLoopGroup();
  private final MemoryRequestChannel<CommandRpcRequest<?>, CommandReply> commandRequestChannel =
      new MemoryRequestChannel<>();
  private final ChannelListener<CommandRpcRequest<?>> commandRequestListener = waitForReply(commandRequestChannel);
  private final Random random = new Random();

  private static int CONTROL_PORT = 9999;

  {
    CONTROL_PORT += random.nextInt(1024);
  }

  private final ControlModule controlModule = new ControlService(c5Server,
      serviceFiber,
      acceptConnectionGroup,
      ioWorkerGroup,
      CONTROL_PORT);

  @Before
  public void setupCommonExpectationsAndStartControlModule() throws Exception {
    List<String> addresses = Lists.newArrayList("127.0.0.1");
    long nodeId = 1;
    List<Long> fakePeers = Arrays.asList(nodeId);

    context.checking(new Expectations() {{
      allowing(hRegionTablet).getRegion();
      will(returnValue(region));

      allowing(hRegionTablet).getPeers();
      will(returnValue(fakePeers));

      allowing(c5Server).getModule(ModuleType.Discovery);
      will(returnFutureWithValue(discoveryModule));

      allowing(c5Server).getModule(ModuleType.ControlRpc);
      will(returnFutureWithValue(controlModule));

      allowing(c5Server).getNodeId();
      will(returnValue(nodeId));

      allowing(c5Server).getCommandRequests();
      will(returnValue(commandRequestChannel));

      allowing(region).getScanner(with(any(Scan.class)));
      will(returnValue(regionScanner));

      allowing(discoveryModule).getNodeInfo(with(any(long.class)), with(any(ModuleType.class)));
      will(returnFutureWithValue(new NodeInfoReply(true, addresses, CONTROL_PORT)));
    }});

    controlModule.start().get();
  }

  @Test
  public void shouldBootStrapMetaOnlyWhenRootIsBlank() throws Throwable {
    long numberOfMetaPeers = 1;

    context.checking(new Expectations() {{
      oneOf(regionScanner).next(with(any(List.class)), with(any(int.class)));
      will(returnValue(true));

      oneOf(region).mutate(with(any(MutationProto.class)), with(any(Condition.class)));
      will(returnValue(true));
    }});

    RootTabletLeaderBehavior rootTabletLeaderBehavior =
        new RootTabletLeaderBehavior(hRegionTablet, c5Server, numberOfMetaPeers);
    rootTabletLeaderBehavior.start();

    assertEventually(commandRequestListener, CommandMatchers.hasMessageWithRPC(C5ServerConstants.START_META));
  }

  @Test
  public void shouldSkipBootStrapMetaOnlyWhenRootIsNotBlank() throws Throwable {
    context.checking(new Expectations() {{
      oneOf(regionScanner).next(with(any(List.class)), with(any(int.class)));
      will(AddElementsActionReturnTrue.addElements(new Cell()));

      never(region).mutate(with(any(MutationProto.class)), with(any(Condition.class)));
    }});

    RootTabletLeaderBehavior rootTabletLeaderBehavior = new RootTabletLeaderBehavior(hRegionTablet,
        c5Server, C5ServerConstants.DEFAULT_QUORUM_SIZE);
    rootTabletLeaderBehavior.start();
  }
}