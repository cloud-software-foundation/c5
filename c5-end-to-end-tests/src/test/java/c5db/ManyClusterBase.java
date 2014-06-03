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
import c5db.client.scanner.ClientScannerManager;
import c5db.interfaces.C5Module;
import c5db.interfaces.C5Server;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.interfaces.tablet.Tablet;
import c5db.interfaces.tablet.TabletStateChange;
import c5db.messages.generated.ModuleSubCommand;
import c5db.messages.generated.ModuleType;
import c5db.regionserver.scan.ScannerManager;
import c5db.tablet.TabletService;
import com.google.common.util.concurrent.Service;
import io.protostuff.ByteString;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
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
import sun.misc.BASE64Encoder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

public class ManyClusterBase {
  static int metaOnPort;
  private static Channel<CommandRpcRequest<?>> commandChannel;
  private static List<C5Server> servers = new ArrayList<>();
  @ClassRule
  public static TemporaryFolder testFolder = new TemporaryFolder();

  @Rule
  public TestName name = new TestName();
  private FakeHTable table;
  private byte[] row;
  private int userTabletOn;

  @AfterClass()
  public static void afterClass() throws ExecutionException, InterruptedException, TimeoutException {

    Log.warn("-----------------------------------------------------------------------------------------------------------");
    for (C5Server server : servers) {
      for (C5Module module : server.getModules().values()) {
        module.stopAndWait();
      }
      Service.State state = server.stopAndWait();
    }
    Log.warn("-----------------------------------------------------------------------------------------------------------");
    ScannerManager.INSTANCE.clearAll();
    ClientScannerManager.INSTANCE.clearAll();
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    Log.warn("-----------------------------------------------------------------------------------------------------------");
    System.setProperty(C5ServerConstants.CLUSTER_NAME_PROPERTY_NAME, String.valueOf("foo"));
    testFolder.create();

    int processors = Runtime.getRuntime().availableProcessors();
    PoolFiberFactory fiberPool = new PoolFiberFactory(Executors.newFixedThreadPool(processors));
    Random random = new Random();

    final CountDownLatch latch = new CountDownLatch(1);
    for (int i = 0; i != 3; i++) {
      Path nodeBasePath = Paths.get(testFolder.getRoot().getAbsolutePath(), String.valueOf(random.nextInt()));
      new File(nodeBasePath.toUri()).mkdirs();
      System.setProperty(C5ServerConstants.C5_CFG_PATH, nodeBasePath.toString());

      System.setProperty(C5ServerConstants.WEB_SERVER_PORT_PROPERTY_NAME,
          String.valueOf(31337 + random.nextInt(1000)));
      System.setProperty(C5ServerConstants.CONTROL_SERVER_PORT_PROPERTY_NAME, String.valueOf(20000 + random.nextInt(1000)));

      C5Server server = Main.startC5Server(new String[]{});
      servers.add(server);
      // create java.util.concurrent.CountDownLatch to notify when message arrives
      C5Module regionServer = server.getModule(ModuleType.RegionServer).get();
      C5Module tabletServer = server.getModule(ModuleType.Tablet).get();
      Fiber fiber = fiberPool.create();
      fiber.start();
      ((TabletService) tabletServer).getTabletStateChanges().subscribe(fiber, tabletStateChange -> {
        if (tabletStateChange.state.equals(Tablet.State.Leader)) {
          if (tabletStateChange.tablet.getRegionInfo().getRegionNameAsString().startsWith("hbase:meta")) {
            metaOnPort = regionServer.port();
            System.out.println("Found meta on:" + metaOnPort);
            commandChannel = server.getCommandChannel();
            latch.countDown();
          }
        }
      });
    }

    latch.await();
    fiberPool.dispose();
    Log.warn("-----------------------------------------------------------------------------------------------------------");
  }

  @Before
  public void before() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    Fiber receiver = new ThreadFiber();
    receiver.start();

    final CountDownLatch latch = new CountDownLatch(1);
    final ByteString tableName = ByteString.copyFrom(Bytes.toBytes(name.getMethodName()));

    for (C5Server server : servers) {
      C5Module regionServer = server.getModule(ModuleType.RegionServer).get();
      C5Module tabletServer = server.getModule(ModuleType.Tablet).get();

      Callback<TabletStateChange> onMsg = message -> {
        if (!message.tablet.getTableDescriptor().getTableName().getNameAsString().startsWith("hbase:")
            && message.state.equals(Tablet.State.Leader)) {
          userTabletOn = regionServer.port();
          latch.countDown();
        }
      };
      ((TabletService) tabletServer).getTabletStateChanges().subscribe(receiver, onMsg);
    }

    for (C5Server server : servers) {
      ModuleSubCommand createTableSubCommand = new ModuleSubCommand(ModuleType.Tablet,
          getCreateTabletSubCommand(tableName));
      commandChannel.publish(new CommandRpcRequest<>(server.getNodeId(), createTableSubCommand));
    }

    // create java.util.concurrent.CountDownLatch to notify when message arrives
    latch.await();

    table = new FakeHTable(C5TestServerConstants.LOCALHOST, userTabletOn, tableName);
    row = Bytes.toBytes(name.getMethodName());
    receiver.dispose();
  }


  protected int getRegionServerPort() {
    Log.info("Getting region from: " + userTabletOn);
    return userTabletOn;
  }

  String getCreateTabletSubCommand(ByteString tableNameBytes) {
    TableName tableName = TableName.valueOf(tableNameBytes.toByteArray());
    HTableDescriptor testDesc = new HTableDescriptor(tableName);
    testDesc.addFamily(new HColumnDescriptor("cf"));
    HRegionInfo testRegion = new HRegionInfo(tableName, new byte[]{0}, new byte[]{}, false, 1);

    String peerString = String.valueOf(servers.get(0).getNodeId() + ","
        + servers.get(1).getNodeId() + ","
        + servers.get(2).getNodeId());
    BASE64Encoder encoder = new BASE64Encoder();

    String hTableDesc = encoder.encodeBuffer(testDesc.toByteArray());
    String hRegionInfo = encoder.encodeBuffer(testRegion.toByteArray());

    return C5ServerConstants.CREATE_TABLE + ":" + hTableDesc + "," + hRegionInfo + "," + peerString;

  }

  @After
  public void after() throws InterruptedException, IOException {
    table.close();
  }

}