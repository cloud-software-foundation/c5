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
package c5db;

import c5db.client.FakeHTable;
import c5db.client.generated.TableName;
import c5db.interfaces.C5Module;
import c5db.interfaces.C5Server;
import c5db.interfaces.RegionServerModule;
import c5db.interfaces.TabletModule;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.interfaces.tablet.Tablet;
import c5db.interfaces.tablet.TabletStateChange;
import c5db.messages.generated.ModuleSubCommand;
import c5db.messages.generated.ModuleType;
import c5db.util.TabletNameHelpers;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetlang.channels.Channel;
import org.jetlang.core.Callback;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;
import org.jetlang.fibers.ThreadFiber;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.mortbay.log.Log;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ClusterOrPseudoCluster {
  public static final byte[] value = Bytes.toBytes("value");
  protected static final byte[] notEqualToValue = Bytes.toBytes("notEqualToValue");

  @ClassRule
  public static TemporaryFolder testFolder = new TemporaryFolder();
  private static Channel<TabletStateChange> stateChanges;
  private static C5Server server;
  static int metaOnPort;
  private static long metaOnNode;

  private static boolean dirty = true;

  @Rule
  public TestName name = new TestName();
  protected FakeHTable table;
  protected byte[] row;
  public final byte[][] splitkeys = new byte[][]{};

  protected static int getRegionServerPort() throws ExecutionException, InterruptedException {
    return server.getModule(ModuleType.RegionServer).get().port();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (dirty) {
      for (C5Module module : server.getModules().values()) {
        try {
          module.stop().get(1, TimeUnit.SECONDS);
        } catch (UncheckedExecutionException e) {
          e.printStackTrace();
        }
      }
      server.stop().get(1, TimeUnit.SECONDS);
      Log.warn("We left it dirty");
    }
  }

  public static void makeDirty() {
    dirty = true;
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    if (dirty) {
      System.setProperty(C5ServerConstants.C5_CFG_PATH, ClusterOrPseudoCluster.testFolder.getRoot().getAbsolutePath());
      System.setProperty("clusterName", C5ServerConstants.LOCALHOST);

    server = Main.startC5Server(new String[]{});
    metaOnNode = server.getNodeId();
    TabletModule tabletServer = (TabletModule) server.getModule(ModuleType.Tablet).get();
    RegionServerModule regionServer = (RegionServerModule) server.getModule(ModuleType.RegionServer).get();
    stateChanges = tabletServer.getTabletStateChanges();

    Fiber receiver = new ThreadFiber();
    receiver.start();

    // create java.util.concurrent.CountDownLatch to notify when message arrives
    final CountDownLatch latch = new CountDownLatch(1);
    int processors = Runtime.getRuntime().availableProcessors();
    PoolFiberFactory fiberPool = new PoolFiberFactory(Executors.newFixedThreadPool(processors));
    Fiber fiber = fiberPool.create();
    fiber.start();
    tabletServer.getTabletStateChanges().subscribe(fiber, tabletStateChange -> {
      if (tabletStateChange.state.equals(Tablet.State.Leader)) {
        if (tabletStateChange.tablet.getRegionInfo().getRegionNameAsString().startsWith("hbase:meta")) {
          metaOnPort = regionServer.port();
          metaOnNode = server.getNodeId();

          latch.countDown();
          fiber.dispose();
        }
      }
    });

    latch.await();
    receiver.dispose();
    }
    dirty = false;
  }

  @After
  public void after() throws InterruptedException {
    if (dirty) {
      table.close();
    }
  }

  @Before
  public void before() throws InterruptedException, ExecutionException, TimeoutException {
      Fiber receiver = new ThreadFiber();
      receiver.start();

      final CountDownLatch latch = new CountDownLatch(1);
      Callback<TabletStateChange> onMsg = message -> {
        if (message.state.equals(Tablet.State.Leader)) {
          latch.countDown();
        }
      };
      stateChanges.subscribe(receiver, onMsg);

      TableName clientTableName = TabletNameHelpers.getClientTableName("c5", name.getMethodName());
      org.apache.hadoop.hbase.TableName tableName = TabletNameHelpers.getHBaseTableName(clientTableName);
      Channel<CommandRpcRequest<?>> commandChannel = server.getCommandChannel();

      ModuleSubCommand createTableSubCommand = new ModuleSubCommand(ModuleType.Tablet,
          TestHelpers.getCreateTabletSubCommand(tableName, splitkeys, Arrays.asList(server)));
      CommandRpcRequest<ModuleSubCommand> createTableCommand = new CommandRpcRequest<>(server.getNodeId(),
          createTableSubCommand);

      commandChannel.publish(createTableCommand);
      latch.await();

      table = new FakeHTable(C5TestServerConstants.LOCALHOST, getRegionServerPort(),
          TabletNameHelpers.toByteString(clientTableName));
      row = Bytes.toBytes(name.getMethodName());
      receiver.dispose();
  }
}