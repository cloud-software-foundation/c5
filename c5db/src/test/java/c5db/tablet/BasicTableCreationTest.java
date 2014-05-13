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

import c5db.C5ServerConstants;
import c5db.ConfigDirectory;
import c5db.TestHelpers;
import c5db.interfaces.C5Server;
import c5db.interfaces.DiscoveryModule;
import c5db.interfaces.ReplicationModule;
import c5db.interfaces.replication.Replicator;
import c5db.interfaces.tablet.Tablet;
import c5db.interfaces.tablet.TabletStateChange;
import c5db.messages.generated.ModuleType;
import c5db.util.C5FiberFactory;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import io.protostuff.ByteString;
import org.apache.hadoop.hbase.client.Put;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.core.Callback;
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

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class BasicTableCreationTest {

  @Rule
  public final JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(new Synchroniser());
  }};

  private final int QUORUM_SIZE = 100;
  private final SettableFuture nodeNotificationsCallback = SettableFuture.create();

  private final C5Server c5Server = context.mock(C5Server.class);
  private final C5FiberFactory c5FiberFactory = context.mock(C5FiberFactory.class);
  private final DiscoveryModule discoveryModule = context.mock(DiscoveryModule.class);
  private final ReplicationModule replicationModule = context.mock(ReplicationModule.class);
  private final ConfigDirectory configDirectory = context.mock(ConfigDirectory.class);
  private final Channel nodeNotifications = context.mock(Channel.class);
  private final Region region = context.mock(Region.class);
  private final SettableFuture<DiscoveryModule> discoveryModuleFuture = SettableFuture.create();
  private final SettableFuture<ReplicationModule> replicatorModuleFuture = SettableFuture.create();
  private final MemoryChannel stateChangeChannel = new MemoryChannel();
  private final MemoryChannel stateChannel = new MemoryChannel();
  Replicator replicator = context.mock(Replicator.class);


  private TabletService tabletService;

  private PoolFiberFactory poolFiberFactory;


  @Before
  public void before() throws IOException, ExecutionException, InterruptedException {
    poolFiberFactory = new PoolFiberFactory(Executors.newSingleThreadExecutor());
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

        oneOf(c5FiberFactory).create();
        will(returnValue(poolFiberFactory.create()));

        oneOf(c5Server).getModule(ModuleType.Discovery);
        will(returnValue(discoveryModuleFuture));

        oneOf(c5Server).getModule(ModuleType.Replication);
        will(returnValue(replicatorModuleFuture));

        oneOf(c5Server).getConfigDirectory();
        will(returnValue(configDirectory));

        // Emulate a very large quorum
        oneOf(c5Server).isSingleNodeMode();
        will(returnValue(false));

        oneOf(c5Server).getMinQuorumSize();
        will(returnValue(QUORUM_SIZE));

        oneOf(discoveryModule).getNewNodeNotifications();
        will(returnValue(nodeNotifications));

        oneOf(nodeNotifications).subscribe(with.is(anything()), with.is(anything()));

        oneOf(discoveryModule).getState();
        will(returnValue(nodeNotificationsCallback));

        allowing(configDirectory).getBaseConfigPath();
        will(returnValue(Paths.get("/tmp")));

        allowing(configDirectory).writeBinaryData(with(any(String.class)),
            with(any(String.class)),
            with.is(anything()));
        allowing(configDirectory).writePeersToFile(with(any(String.class)), with(any(List.class)));
        allowing(replicationModule).createReplicator(with(any(String.class)), with(any(List.class)));
        will(returnValue(replicatorSettableFuture));

        allowing(replicator).getStateChangeChannel();
        will(returnValue(stateChangeChannel));

        allowing(replicator).getStateChannel();
        will(returnValue(stateChannel));

        allowing(replicator).start();

        allowing(replicator).getQuorumId();
        will(returnValue("hbase:meta,\\x00"));

      }
    });

    ListenableFuture<Service.State> future = tabletService.start();
    discoveryModuleFuture.set(discoveryModule);
    replicatorModuleFuture.set(replicationModule);


    Service.State state = future.get();
    System.out.println(state);
  }

  @After
  public void tearDown() {
    poolFiberFactory.dispose();
  }


  @Test
  public void shouldCreateMetaEntryAppropriatelyOnTableCreation() throws Throwable {
    final Fiber fiber = poolFiberFactory.create();
    context.checking(new Expectations() {
      {
        oneOf(c5FiberFactory).create();
        will(returnValue(fiber));
      }
    });

    ByteString tableName = ByteString.copyFromUtf8("tabletName");
    long nodeId = 1l;

    tabletService.acceptCommand(C5ServerConstants.START_META + ":1,2,3");
    Tablet tablet = tabletService.getTablet("hbase:meta");
    CountDownLatch countDownLatch = new CountDownLatch(1);
    tablet.getStateChangeChannel().subscribe(fiber, new Callback<TabletStateChange>() {
      @Override
      public void onMessage(TabletStateChange message) {
        countDownLatch.countDown();
      }
    });
    countDownLatch.await();

    final Fiber fiber2 = poolFiberFactory.create();
    final Fiber fiber3 = poolFiberFactory.create();

    SettableFuture<Long> logFuture = SettableFuture.create();
    context.checking(new Expectations() {
      {
        oneOf(c5FiberFactory).create();
        will(returnValue(fiber2));

        oneOf(c5FiberFactory).create();
        will(returnValue(fiber3));

        allowing(replicator).logData(with(any(List.class)));
        will(returnValue(logFuture));

      }
    });

    tabletService.acceptCommand(TestHelpers.getCreateTabletSubCommand(tableName, nodeId));
  }
}

