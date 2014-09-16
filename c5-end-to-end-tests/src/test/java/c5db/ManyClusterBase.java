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
import c5db.interfaces.server.CommandRpcRequest;
import c5db.interfaces.tablet.Tablet;
import c5db.interfaces.tablet.TabletStateChange;
import c5db.messages.generated.ModuleSubCommand;
import c5db.messages.generated.ModuleType;
import c5db.tablet.TabletService;
import c5db.util.TabletNameHelpers;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.protostuff.ByteString;
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

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ManyClusterBase {
  static int metaOnPort;
  private static Channel<CommandRpcRequest<?>> commandChannel;
  private static List<C5Server> servers = new ArrayList<>();
  @ClassRule
  public static TemporaryFolder testFolder = new TemporaryFolder();
  private static long metaOnNode;
  static FakeHTable metaTable;

  @Rule
  public TestName name = new TestName();
  protected FakeHTable table;
  protected byte[] row;
  private final Map<String, Integer> userTabletOn = new HashMap<>();

  final byte[][] splitkeys = {};

  @AfterClass()
  public static void afterClass() throws ExecutionException, InterruptedException, TimeoutException {
    Log.warn("-----------------------------------------------------------------------------------------------------------");
    for (C5Server server : servers) {
      for (C5Module module : server.getModules().values()) {
        try {
          module.stop().get(1, TimeUnit.SECONDS);
        } catch (UncheckedExecutionException e) {
          e.printStackTrace();
        }
      }
      server.stop().get(1, TimeUnit.SECONDS);
    }
    servers = new ArrayList<>();
    Log.warn("-----------------------------------------------------------------------------------------------------------");

  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    Log.warn("-----------------------------------------------------------------------------------------------------------");
    System.setProperty(C5ServerConstants.CLUSTER_NAME_PROPERTY_NAME, String.valueOf("foo"));
    System.setProperty(C5ServerConstants.MIN_CLUSTER_SIZE, String.valueOf(3));
    testFolder.create();

    int processors = Runtime.getRuntime().availableProcessors();
    PoolFiberFactory fiberPool = new PoolFiberFactory(Executors.newFixedThreadPool(processors));
    Random random = new Random();

    final CountDownLatch latch = new CountDownLatch(1);
    for (int i = 0; i != 3; i++) {
      Path nodeBasePath = Paths.get(testFolder.getRoot().getAbsolutePath(), String.valueOf(random.nextInt()));
      boolean success = new File(nodeBasePath.toUri()).mkdirs();
      if (!success) {
        throw new IOException("Unable to create: " + nodeBasePath);
      }
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
            metaOnNode = server.getNodeId();
            try {
              metaTable = new FakeHTable("localhost",
                  metaOnPort,
                  ByteString.copyFromUtf8("hbase:meta"));
            } catch (InterruptedException | TimeoutException | ExecutionException e) {
              e.printStackTrace();
            }

            commandChannel = server.getCommandChannel();
            latch.countDown();
            fiber.dispose();
          }
        }
      });
    }

    latch.await();
    fiberPool.dispose();
    Log.warn("-----------------------------------------------------------------------------------------------------------");
  }

  @Before
  public void before() throws InterruptedException, ExecutionException, TimeoutException, IOException, URISyntaxException {
    Fiber receiver = new ThreadFiber();
    receiver.start();

    final CountDownLatch latch = new CountDownLatch(1);
    final String tableName = name.getMethodName();

    for (C5Server server : servers) {
      C5Module regionServer = server.getModule(ModuleType.RegionServer).get();
      C5Module tabletServer = server.getModule(ModuleType.Tablet).get();

      Callback<TabletStateChange> onMsg = message -> {
        if (!message.tablet.getTableDescriptor().getTableName().getNameAsString().startsWith("hbase:")
            && message.state.equals(Tablet.State.Leader)) {
          userTabletOn.put(message.tablet.getRegionInfo().getRegionNameAsString(), regionServer.port());
          latch.countDown();
        }
      };
      ((TabletService) tabletServer).getTabletStateChanges().subscribe(receiver, onMsg);
    }

    ModuleSubCommand createTableSubCommand = new ModuleSubCommand(ModuleType.Tablet,
        TestHelpers.getCreateTabletSubCommand("c5", tableName, splitkeys, servers));
    commandChannel.publish(new CommandRpcRequest<>(metaOnNode, createTableSubCommand));

    // create java.util.concurrent.CountDownLatch to notify when message arrives
    latch.await();
    TableName clientTableName = TabletNameHelpers.getClientTableName("c5", name.getMethodName());

    table = new FakeHTable(C5TestServerConstants.LOCALHOST,
        userTabletOn.values().iterator().next(),
        TabletNameHelpers.toByteString(clientTableName));
    row = Bytes.toBytes(name.getMethodName());
    receiver.dispose();
  }

  @After
  public void after() throws InterruptedException {
    userTabletOn.clear();
    table.close();
  }

}