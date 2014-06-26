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
import c5db.interfaces.TabletModule;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.interfaces.tablet.Tablet;
import c5db.interfaces.tablet.TabletStateChange;
import c5db.messages.generated.ModuleSubCommand;
import c5db.messages.generated.ModuleType;
import c5db.util.TabletNameHelpers;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetlang.channels.Channel;
import org.jetlang.core.Callback;
import org.jetlang.fibers.Fiber;
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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MiniClusterBase {
  public static final byte[] value = Bytes.toBytes("value");
  protected static final byte[] notEqualToValue = Bytes.toBytes("notEqualToValue");

  @ClassRule
  public static TemporaryFolder testFolder = new TemporaryFolder();
  private static Channel<TabletStateChange> stateChanges;
  private static C5Server server;
  private static int metaOnPort;
  private static long metaOnNode;
  static FakeHTable metaTable;
  @Rule
  public TestName name = new TestName();
  public static FakeHTable table;
  protected byte[] row;
  byte[][] splitkeys = {Bytes.toBytes(10), Bytes.toBytes(100), Bytes.toBytes(1000), Bytes.toBytes(10000)};

  protected static int getRegionServerPort() throws ExecutionException, InterruptedException {
    return server.getModule(ModuleType.RegionServer).get().port();
  }

  @AfterClass
  public static void afterClass() throws InterruptedException, ExecutionException, TimeoutException {
    ImmutableMap<ModuleType, C5Module> modules = null;

    modules = server.getModules();

    if (modules != null) {
      for (C5Module module : modules.values()) {
        module.stopAndWait();
      }
    }
    server.stop().get(1, TimeUnit.SECONDS);
    Log.warn("-----------------------------------------------------------------------------------------------------------");
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    Log.warn("-----------------------------------------------------------------------------------------------------------");

    System.setProperty(C5ServerConstants.C5_CFG_PATH, MiniClusterBase.testFolder.getRoot().getAbsolutePath());
    System.setProperty("clusterName", C5ServerConstants.LOCALHOST);

    server = Main.startC5Server(new String[]{});
    ListenableFuture<C5Module> tabletServerFuture = server.getModule(ModuleType.Tablet);
    TabletModule tabletServer = (TabletModule) tabletServerFuture.get(1, TimeUnit.SECONDS);
    stateChanges = tabletServer.getTabletStateChanges();

    Fiber receiver = new ThreadFiber();
    receiver.start();

    // create java.util.concurrent.CountDownLatch to notify when message arrives
    final CountDownLatch latch = new CountDownLatch(2);

    Callback<TabletStateChange> onMsg = message -> {
      if (message.tablet.getTableDescriptor().getTableName().getNameAsString().startsWith("hbase:")
          && message.state.equals(Tablet.State.Leader)) {
        latch.countDown();
      }
    };
    (tabletServer).getTabletStateChanges().subscribe(receiver, onMsg);
    latch.await();
  }

  @After
  public void after() throws InterruptedException, IOException {
    table.close();
    metaTable.close();
  }

  @Before
  public void before()
      throws InterruptedException, ExecutionException, TimeoutException, IOException, URISyntaxException {
    Fiber receiver = new ThreadFiber();
    receiver.start();

    final CountDownLatch latch = new CountDownLatch(splitkeys.length + 1);
    Callback<TabletStateChange> onMsg = message -> {
      if (!message.tablet.getTableDescriptor().getTableName().getNameAsString().startsWith("hbase:")
          && message.state.equals(Tablet.State.Leader)) {
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

    table = new FakeHTable(C5TestServerConstants.LOCALHOST, getRegionServerPort(), clientTableName);
    row = Bytes.toBytes(name.getMethodName());
    table.put(new Put(row));
    receiver.dispose();
  }
}