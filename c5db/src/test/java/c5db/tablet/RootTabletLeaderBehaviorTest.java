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
package c5db.tablet;

import c5db.AsyncChannelAsserts;
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
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static c5db.AsyncChannelAsserts.assertEventually;
import static c5db.AsyncChannelAsserts.waitForReply;
import static c5db.FutureActions.returnFutureWithValue;

public class RootTabletLeaderBehaviorTest {
  Synchroniser sync = new Synchroniser();
  @Rule
  public final JUnitRuleMockery context = new JUnitRuleMockery() {{

    setThreadingPolicy(sync);
  }};
  private c5db.interfaces.tablet.Tablet hRegionTablet = context.mock(Tablet.class, "mockHRegionTablet");
  private Region region = context.mock(Region.class, "mockRegion");
  private C5Server c5Server = context.mock(C5Server.class, "mockC5Server");
  private DiscoveryModule discoveryModule = context.mock(DiscoveryModule.class);
  int processors = Runtime.getRuntime().availableProcessors();
  ExecutorService executors = Executors.newFixedThreadPool(processors);
  PoolFiberFactory fiberPool = new PoolFiberFactory(executors);
  Fiber serviceFiber = fiberPool.create();
  private final NioEventLoopGroup acceptConnectionGroup = new NioEventLoopGroup(1);
  private final NioEventLoopGroup ioWorkerGroup = new NioEventLoopGroup();

  Random random = new Random();
  private static int CONTROL_PORT = 9999;

  {
    CONTROL_PORT += random.nextInt(1024);
  }

  private ControlModule controlModule = new ControlService(c5Server,
      serviceFiber,
      acceptConnectionGroup,
      ioWorkerGroup,
      CONTROL_PORT);
  private AsyncChannelAsserts.ChannelListener commandListener;


  @Before
  public void before() throws InterruptedException, ExecutionException {
    context.checking(new Expectations() {
      {
        oneOf(c5Server).getModule(ModuleType.Discovery);
        will(returnFutureWithValue(discoveryModule));


      }
    });
    controlModule.start().get();
  }

  @Test
  public void shouldBootStrapMetaOnlyWhenRootIsBlank() throws Throwable {
    List<Long> fakePeers = Arrays.asList(1l);
    MemoryRequestChannel<CommandRpcRequest<?>, CommandReply> memoryChannel = new MemoryRequestChannel<>();
    RegionScanner regionScanner = context.mock(RegionScanner.class);

    List<String> addresses = new ArrayList<>();
    addresses.add("127.0.0.1");

    context.checking(new Expectations() {
      {
        oneOf(hRegionTablet).getRegion();
        will(returnValue(region));

        oneOf(region).getScanner(with(any(Scan.class)));
        will(returnValue(regionScanner));

        oneOf(regionScanner).next(with(any(List.class)), with(any(int.class)));

        oneOf(hRegionTablet).getPeers();
        will(returnValue(fakePeers));

        oneOf(region).mutate(with(any(MutationProto.class)), with(any(Condition.class)));
        will(returnValue(true));

        oneOf(c5Server).getModule(ModuleType.ControlRpc);
        will(returnFutureWithValue(controlModule));

        oneOf(c5Server).getNodeId();
        will(returnValue(1l));

        oneOf(discoveryModule).getNodeInfo(with(any(long.class)), with(any(ModuleType.class)));
        will(returnFutureWithValue(new NodeInfoReply(true, addresses, CONTROL_PORT)));

        oneOf(c5Server).getCommandRequests();
        will(returnValue(memoryChannel));

      }
    });

    commandListener = waitForReply(memoryChannel);
    RootTabletLeaderBehavior rootTabletLeaderBehavior = new RootTabletLeaderBehavior(hRegionTablet, c5Server);
    rootTabletLeaderBehavior.start();
    assertEventually(commandListener, CommandMatchers.hasMessageWithRPC(C5ServerConstants.START_META));
  }

  @Test
  public void shouldSendStartMetaPacketsToTheRightNumberOfPeers() throws Throwable {
    MemoryRequestChannel<CommandRpcRequest<?>, CommandReply> memoryChannel = new MemoryRequestChannel<>();

    List<String> addresses = new ArrayList<>();
    addresses.add("127.0.0.1");
    RegionScanner regionScanner = context.mock(RegionScanner.class);

    List<Long> fakePeers = Arrays.asList(1l);
    context.checking(new Expectations() {{
      oneOf(hRegionTablet).getRegion();
      will(returnValue(region));

      oneOf(region).getScanner(with(any(Scan.class)));
      will(returnValue(regionScanner));

      oneOf(regionScanner).next(with(any(List.class)), with(any(int.class)));

      oneOf(hRegionTablet).getPeers();
      will(returnValue(fakePeers));

      oneOf(region).mutate(with(any(MutationProto.class)), with(any(Condition.class)));
      will(returnValue(true));

      oneOf(c5Server).getModule(ModuleType.ControlRpc);
      will(returnFutureWithValue(controlModule));

      oneOf(c5Server).getNodeId();
      will(returnValue(1l));

      oneOf(discoveryModule).getNodeInfo(with(any(long.class)), with(any(ModuleType.class)));
      will(returnFutureWithValue(new NodeInfoReply(true, addresses, CONTROL_PORT)));

      oneOf(c5Server).getCommandRequests();
      will(returnValue(memoryChannel));

    }});
    commandListener = waitForReply(memoryChannel);

    RootTabletLeaderBehavior rootTabletLeaderBehavior = new RootTabletLeaderBehavior(hRegionTablet, c5Server);

    rootTabletLeaderBehavior.start();
    assertEventually(commandListener, CommandMatchers.hasMessageWithRPC(C5ServerConstants.START_META));
  }

  @Test
  public void shouldSkipBootStrapMetaOnlyWhenRootIsNotBlank() throws Throwable {
    RegionScanner regionScanner = context.mock(RegionScanner.class);
    context.checking(new Expectations() {{

      oneOf(hRegionTablet).getRegion();
      will(returnValue(region));

      oneOf(region).getScanner(with(any(Scan.class)));
      will(returnValue(regionScanner));

      oneOf(regionScanner).next(with(any(List.class)), with(any(int.class)));
      will(AddElementsActionReturnTrue.addElements(new Cell()));

      never(hRegionTablet).getPeers();
      never(c5Server).isSingleNodeMode();
      never(region).mutate(with(any(MutationProto.class)), with(any(Condition.class)));
    }});

    RootTabletLeaderBehavior rootTabletLeaderBehavior = new RootTabletLeaderBehavior(hRegionTablet, c5Server);
    rootTabletLeaderBehavior.start();

  }
}