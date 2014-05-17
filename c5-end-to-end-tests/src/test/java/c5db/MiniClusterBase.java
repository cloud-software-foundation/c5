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
import c5db.interfaces.C5Module;
import c5db.interfaces.C5Server;
import c5db.interfaces.TabletModule;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.interfaces.tablet.Tablet;
import c5db.interfaces.tablet.TabletStateChange;
import c5db.messages.generated.ModuleSubCommand;
import c5db.messages.generated.ModuleType;
import com.google.common.util.concurrent.ListenableFuture;
import io.protostuff.ByteString;
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

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class MiniClusterBase {

  public static final byte[] value = Bytes.toBytes("value");
  protected static final byte[] notEqualToValue = Bytes.toBytes("notEqualToValue");
  private static final Random rnd = new Random();
  @ClassRule
  public static TemporaryFolder testFolder = new TemporaryFolder();
  private static Channel<TabletStateChange> stateChanges;
  private static C5Server server;
  @Rule
  public TestName name = new TestName();
  protected FakeHTable table;
  protected byte[] row;

  protected static int getRegionServerPort() throws ExecutionException, InterruptedException {
    return server.getModule(ModuleType.RegionServer).get().port();
  }

  @AfterClass
  public static void afterClass() throws InterruptedException, ExecutionException, TimeoutException {
    server.stopAndWait();
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    Log.warn("-----------------------------------------------------------------------------------------------------------");

    System.setProperty(C5ServerConstants.C5_CFG_PATH, MiniClusterBase.testFolder.getRoot().getAbsolutePath());
    int webServerPort = 9091 + rnd.nextInt(100);
    System.setProperty("clusterName", C5ServerConstants.LOCALHOST);

    System.setProperty(C5ServerConstants.WEB_SERVER_PORT_PROPERTY_NAME, String.valueOf(webServerPort));

    server = Main.startC5Server(new String[]{});
    ListenableFuture<C5Module> tabletServerFuture = server.getModule(ModuleType.Tablet);
    TabletModule tabletServer = (TabletModule) tabletServerFuture.get();
    stateChanges = tabletServer.getTabletStateChanges();

    Fiber receiver = new ThreadFiber();
    receiver.start();

    // create java.util.concurrent.CountDownLatch to notify when message arrives
    final CountDownLatch latch = new CountDownLatch(2);

    Callback<TabletStateChange> onMsg = message -> {
      if (message.state.equals(Tablet.State.Leader)) {
        latch.countDown();
      }
    };
    stateChanges.subscribe(receiver, onMsg);

    latch.await();
    receiver.dispose();
  }

  @After
  public void after() throws InterruptedException {
    table.close();
  }

  @Before
  public void before() throws InterruptedException {
    Fiber receiver = new ThreadFiber();
    receiver.start();

    final CountDownLatch latch = new CountDownLatch(1);
    Callback<TabletStateChange> onMsg = message -> {
      if (message.state.equals(Tablet.State.Open) || message.state.equals(Tablet.State.Leader)) {
        latch.countDown();
      }
    };
    stateChanges.subscribe(receiver, onMsg);

    final ByteString tableName = ByteString.copyFrom(Bytes.toBytes(name.getMethodName()));
    Channel<CommandRpcRequest<?>> commandChannel = server.getCommandChannel();

    ModuleSubCommand createTableSubCommand = new ModuleSubCommand(ModuleType.Tablet,
        TestHelpers.getCreateTabletSubCommand(tableName, server.getNodeId()));
    CommandRpcRequest<ModuleSubCommand> createTableCommand = new CommandRpcRequest<>(server.getNodeId(),
        createTableSubCommand);

    commandChannel.publish(createTableCommand);
    latch.await();

    try {
      table = new FakeHTable(C5TestServerConstants.LOCALHOST, getRegionServerPort(), tableName);
    } catch (TimeoutException | ExecutionException e) {
      e.printStackTrace();
    }
    row = Bytes.toBytes(name.getMethodName());
    receiver.dispose();
  }
}