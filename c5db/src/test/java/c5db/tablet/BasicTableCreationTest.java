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
import c5db.ConfigDirectory;
import c5db.TestHelpers;
import c5db.discovery.generated.Availability;
import c5db.interfaces.C5Server;
import c5db.interfaces.DiscoveryModule;
import c5db.interfaces.ReplicationModule;
import c5db.interfaces.discovery.NewNodeVisible;
import c5db.interfaces.discovery.NodeInfo;
import c5db.interfaces.replication.Replicator;
import c5db.interfaces.replication.ReplicatorInstanceEvent;
import c5db.interfaces.tablet.Tablet;
import c5db.interfaces.tablet.TabletStateChange;
import c5db.messages.generated.ModuleType;
import c5db.util.C5FiberFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import io.protostuff.ByteString;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static c5db.AsyncChannelAsserts.assertEventually;
import static c5db.AsyncChannelAsserts.listenTo;
import static c5db.TabletMatchers.hasMessageWithState;

public class BasicTableCreationTest {

  @Rule
  public final JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(new Synchroniser());
  }};

  private final int QUORUM_SIZE = 1;
  private final SettableFuture<DiscoveryModule> discoveryModuleFuture = SettableFuture.create();
  private final SettableFuture<ImmutableMap<Long, NodeInfo>> nodeNotificationsCallback = SettableFuture.create();
  private final SettableFuture<ReplicationModule> replicatorModuleFuture = SettableFuture.create();
  private final MemoryChannel<ReplicatorInstanceEvent> eventChannel = new MemoryChannel<>();
  private final MemoryChannel<Replicator.State> stateChannel = new MemoryChannel<>();
  private final MemoryChannel<NewNodeVisible> nodeNotifications = new MemoryChannel<>();
  private final C5Server c5Server = context.mock(C5Server.class);
  private final C5FiberFactory c5FiberFactory = context.mock(C5FiberFactory.class);
  private final DiscoveryModule discoveryModule = context.mock(DiscoveryModule.class);
  private final ReplicationModule replicationModule = context.mock(ReplicationModule.class);
  private final ConfigDirectory configDirectory = context.mock(ConfigDirectory.class);
  private final Replicator replicator = context.mock(Replicator.class);
  private final PoolFiberFactory poolFiberFactory = new PoolFiberFactory(Executors.newSingleThreadExecutor());
  private SettableFuture<Replicator> replicatorSettableFuture = SettableFuture.create();
  private TabletService tabletService;

  @Before
  public void before() throws Throwable {
    context.checking(new Expectations() {
      {
        oneOf(c5Server).getFiberFactory(with(any(Consumer.class)));
        will(returnValue(c5FiberFactory));

        oneOf(c5FiberFactory).create();
        will(returnValue(poolFiberFactory.create()));
      }
    });
    tabletService = new TabletService(c5Server);
    SettableFuture<Replicator> replicatorSettableFuture = SettableFuture.create();
    replicatorSettableFuture.set(replicator);

    context.checking(new Expectations() {
      {

        oneOf(c5Server).getModule(ModuleType.Discovery);
        will(returnValue(discoveryModuleFuture));

        oneOf(c5Server).getModule(ModuleType.Replication);
        will(returnValue(replicatorModuleFuture));

        allowing(c5Server).getConfigDirectory();
        will(returnValue(configDirectory));
        // Emulate a very large quorum
        allowing(c5Server).isSingleNodeMode();
        will(returnValue(false));

        allowing(c5Server).getMinQuorumSize();
        will(returnValue(QUORUM_SIZE));

        oneOf(discoveryModule).getNewNodeNotifications();
        will(returnValue(nodeNotifications));

        allowing(configDirectory).getBaseConfigPath();
        will(returnValue(Paths.get("/tmp")));

        allowing(configDirectory).writeBinaryData(with(any(String.class)),
            with(any(String.class)),
            with.is(anything()));
        allowing(configDirectory).writePeersToFile(with(any(String.class)), with(any(List.class)));

        allowing(replicator).getEventChannel();
        will(returnValue(eventChannel));
      }
    });

    ListenableFuture<Service.State> future = tabletService.start();

    discoveryModuleFuture.set(discoveryModule);

    context.checking(new Expectations() {{
      oneOf(discoveryModule).getState();
      will(returnValue(nodeNotificationsCallback));
    }});

    Map<Long, NodeInfo> nodeStates = new HashMap<>();
    nodeStates.put(1l, new NodeInfo(new Availability()));
    nodeNotificationsCallback.set(ImmutableMap.copyOf(nodeStates));

    Fiber fiber = poolFiberFactory.create();
    context.checking(new Expectations() {{
      // Emulate a very large quorum

      oneOf(c5FiberFactory).create();
      will(returnValue(fiber));

      oneOf(replicationModule).createReplicator(with(any(String.class)), with(any(List.class)));
      will(returnValue(replicatorSettableFuture));

      oneOf(replicator).getStateChannel();
      will(returnValue(stateChannel));

      allowing(replicator).getCommitNoticeChannel();

      allowing(replicator).getId();

      allowing(replicator).getQuorumId();
      will(returnValue("hbase:root,\\x00,1.33578e495f8173aac4be480afe41410a."));
    }});
    AsyncChannelAsserts.ChannelListener<TabletStateChange> tabletStateChangeListener
        = listenTo(tabletService.getTabletStateChanges());

    replicatorModuleFuture.set(replicationModule);
    future.get();
    assertEventually(tabletStateChangeListener, hasMessageWithState(Tablet.State.Open));

  }

  @After
  public void tearDown() throws ExecutionException, InterruptedException {
    tabletService.stop().get();
    poolFiberFactory.dispose();
  }


  @Test
  public void shouldCreateMetaEntryAppropriatelyOnTableCreation() throws Throwable {

    final Fiber startMetaFiber1 = poolFiberFactory.create();
    replicatorSettableFuture = SettableFuture.create();
    context.checking(new Expectations() {
      {
        oneOf(c5FiberFactory).create();
        will(returnValue(startMetaFiber1));

        oneOf(replicationModule).createReplicator(with(any(String.class)), with(any(List.class)));
        will(returnValue(replicatorSettableFuture));

        oneOf(replicator).getStateChannel();
        will(returnValue(stateChannel));

        allowing(replicator).getQuorumId();
        will(returnValue("hbase:meta,\\x00,1.33578e495f8173aac4be480afe41410a."));

      }
    });

    tabletService.acceptCommand(C5ServerConstants.START_META + ":1");
    replicatorSettableFuture.set(replicator);

    AsyncChannelAsserts.ChannelListener<TabletStateChange> tabletStateChangeListener
        = listenTo(tabletService.getTabletStateChanges());

    replicatorModuleFuture.set(replicationModule);
    assertEventually(tabletStateChangeListener, hasMessageWithState(Tablet.State.Open));

    tabletService.getTablet("hbase:meta");
    final Fiber startUserTableFiber1 = poolFiberFactory.create();

    SettableFuture<Long> longSettableFuture = SettableFuture.create();
    context.checking(new Expectations() {
      {

        oneOf(c5FiberFactory).create();
        will(returnValue(startUserTableFiber1));

        oneOf(replicationModule).createReplicator(with(any(String.class)), with(any(List.class)));
        will(returnValue(replicatorSettableFuture));

        oneOf(replicator).getStateChannel();
        will(returnValue(stateChannel));

        allowing(replicator).getQuorumId();
        will(returnValue("hbase:tabletName,\\x00,1.33578e495f8173aac4be480afe41410a."));

        allowing(replicator).logData(with(any(List.class)));
        will(returnValue(longSettableFuture));
      }
    });
    ByteString tableName = ByteString.copyFromUtf8("tabletName");
    long nodeId = 1l;
    AsyncChannelAsserts.ChannelListener<TabletStateChange> listener = listenTo(tabletService.getTabletStateChanges());

    tabletService.acceptCommand(TestHelpers.getCreateTabletSubCommand(tableName, nodeId));
    replicatorSettableFuture.set(replicator);
    assertEventually(listener, hasMessageWithState(c5db.interfaces.tablet.Tablet.State.Open));

  }
}

