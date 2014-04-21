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

import c5db.ConfigDirectory;
import c5db.interfaces.C5Module;
import c5db.interfaces.C5Server;
import c5db.interfaces.DiscoveryModule;
import c5db.interfaces.ReplicationModule;
import c5db.interfaces.TabletModule;
import c5db.log.OLogShim;
import c5db.messages.generated.ModuleType;
import c5db.util.C5FiberFactory;
import c5db.util.FiberOnly;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.core.Disposable;
import org.jetlang.fibers.Fiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;


/**
 * The main entry point for the service which manages the tablet level lifecycle.
 * The only place Tablets are created.
 */
public class TabletService extends AbstractService implements TabletModule {
  private static final Logger LOG = LoggerFactory.getLogger(TabletService.class);
  private static final int INITIALIZATION_TIME = 1000;

  private final C5FiberFactory fiberFactory;
  private final Fiber fiber;
  private final C5Server server;
  // TODO bring this into this class, and not have an external class.
  //private final OnlineRegions onlineRegions = OnlineRegions.INSTANCE;
  private final Map<String, HRegion> onlineRegions = new HashMap<>();
  private ReplicationModule replicationModule = null;
  private DiscoveryModule discoveryModule = null;
  private final Configuration conf;
  private boolean rootStarted = false;


  public TabletService(C5Server server) {
    this.fiberFactory = server.getFiberFactory(this::notifyFailed);
    this.fiber = fiberFactory.create();
    this.server = server;
    this.conf = HBaseConfiguration.create();
  }

  @Override
  public HRegion getTablet(String tabletName) {
    // TODO ugly hack fix eventually
    while (onlineRegions.size() == 0) {
      try {
        LOG.error("Waiting for regions to come online");
        Thread.sleep(INITIALIZATION_TIME);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    HRegion region = onlineRegions.get(tabletName);
    // TODO remove
    if (region == null) {
      Iterator<HRegion> iterator = onlineRegions.values().iterator();
      do {
        region = iterator.next();
      } while (region.getTableDesc().getTableName().getNameAsString().contains("root"));

    }
    return region;
  }

  @Override
  protected void doStart() {
    fiber.start();

    fiber.execute(() -> {

      ListenableFuture<C5Module> discoveryService = server.getModule(ModuleType.Discovery);
      try {
        discoveryModule = (DiscoveryModule) discoveryService.get();
      } catch (InterruptedException | ExecutionException e) {
        notifyFailed(e);
        return;
      }

      ListenableFuture<C5Module> replicatorService = server.getModule(ModuleType.Replication);
      Futures.addCallback(replicatorService, new FutureCallback<C5Module>() {
        @Override
        public void onSuccess(C5Module result) {
          replicationModule = (ReplicationModule) result;
          fiber.execute(() -> {
            try {
              Path path = server.getConfigDirectory().getBaseConfigPath();


//                                    RegistryFile registryFile = new RegistryFile(path);

//                                    int startCount = startRegions(registryFile);

              // if no regions were started, we need to bootstrap once we have
              // enough online regions.
//                                    if (startCount == 0) {


// TODO start ROOT region instead of boot-strapping root region.
              startBootstrap();


//                                    }

//                                    logReplay(path);

              notifyStarted();
            } catch (Exception e) {
              notifyFailed(e);
            }
          });
        }

        @Override
        public void onFailure(Throwable t) {
          notifyFailed(t);
        }
      }, fiber);
    });

  }

  private Disposable newNodeWatcher = null;


  @FiberOnly
  private void startBootstrap() {
    LOG.info("Waiting to find at least " + getMinQuorumSize() + " nodes to bootstrap with");

    final FutureCallback<ImmutableMap<Long, DiscoveryModule.NodeInfo>> callback = new FutureCallback<ImmutableMap<Long, DiscoveryModule.NodeInfo>>() {
      @Override
      @FiberOnly
      public void onSuccess(ImmutableMap<Long, DiscoveryModule.NodeInfo> result) {
        maybeStartBootstrap(result);
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.warn("failed to get discovery state", t);
      }
    };

    newNodeWatcher = discoveryModule.getNewNodeNotifications().subscribe(fiber, message -> {
      ListenableFuture<ImmutableMap<Long, DiscoveryModule.NodeInfo>> f = discoveryModule.getState();
      Futures.addCallback(f, callback, fiber);
    });

    ListenableFuture<ImmutableMap<Long, DiscoveryModule.NodeInfo>> f = discoveryModule.getState();
    Futures.addCallback(f, callback, fiber);
  }

  @FiberOnly
  private void maybeStartBootstrap(ImmutableMap<Long, DiscoveryModule.NodeInfo> nodes) {
    List<Long> peers = new ArrayList<>(nodes.keySet());

    LOG.debug("Found a bunch of peers: {}", peers);
    if (peers.size() < getMinQuorumSize()) {
      return;
    }

    if (rootStarted) {
      return;
    }
    rootStarted = true;
    bootstrapRoot(ImmutableList.copyOf(peers));
    // TODO REMOVE. Temp table while we have no meta infrastructure
    bootstrapTempTable(ImmutableList.copyOf(peers));
//        // bootstrap the frickin thing.
//        LOG.debug("Bootstrapping empty region");
//        // simple bootstrap, only bootstrap my own ID:
//        byte[] startKey = {0};
//        byte[] endKey = {};
//        TableName tableName = TableName.valueOf("tableName");
//        HRegionInfo hRegionInfo = new HRegionInfo(tableName,
//                startKey, endKey, false, 0);
//        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
//        tableDescriptor.addFamily(new HColumnDescriptor("cf"));
//
//        try {
//            registryFile.addEntry(hRegionInfo, new HColumnDescriptor("cf"), peers);
//        } catch (IOException e) {
//            LOG.error("Cant append to registryFile, not bootstrapping!!!", e);
//            return;
//        }
//
//        openRegion0(hRegionInfo, tableDescriptor, ImmutableList.copyOf(peers));

    if (newNodeWatcher != null) {
      newNodeWatcher.dispose();
      newNodeWatcher = null;
    }
  }

  private void bootstrapTempTable(ImmutableList<Long> peers) {
    TableName tableName = TableName.valueOf("1");
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("cf"));

    HRegionInfo region = new HRegionInfo(tableName);
    openRegion0(region, desc, ImmutableList.copyOf(peers));

  }

  // to bootstrap root we need to find the list of peers we should be connected to, and then do that.
  // how to bootstrap?
  private void bootstrapRoot(List<Long> peers) {
    HTableDescriptor rootDesc = HTableDescriptor.ROOT_TABLEDESC;
    HRegionInfo rootRegion = new HRegionInfo(
        rootDesc.getTableName(), new byte[]{0}, new byte[]{}, false, 1);

    // ok we have enough to start a region up now:

    openRegion0(rootRegion, rootDesc, ImmutableList.copyOf(peers));
  }

  private void openRegion0(final HRegionInfo regionInfo,
                           final HTableDescriptor tableDescriptor,
                           final ImmutableList<Long> peers) {
    LOG.debug("Opening replicator for region {} peers {}", regionInfo, peers);

    String quorumId = regionInfo.getRegionNameAsString();
    ConfigDirectory serverConfigDir = server.getConfigDirectory();

    ListenableFuture<ReplicationModule.Replicator> future =
        replicationModule.createReplicator(quorumId, peers);
    Futures.addCallback(future, new FutureCallback<ReplicationModule.Replicator>() {
      @Override
      @FiberOnly
      public void onSuccess(ReplicationModule.Replicator result) {
        try {
          // TODO subscribe to the replicator's broadcasts.

          result.start();
          OLogShim shim = new OLogShim(result);

          // default place for a region is....
          // tableName/encodedName.
          HRegion region = HRegion.openHRegion(new org.apache.hadoop.fs.Path(serverConfigDir.getBaseConfigPath().toString()),
              regionInfo,
              tableDescriptor,
              shim,
              conf,
              null, null);

          onlineRegions.put(quorumId, region);

          serverConfigDir.writeBinaryData(quorumId, "regionInfo", regionInfo.toDelimitedByteArray());
          serverConfigDir.writePeersToFile(quorumId, peers);
          LOG.debug("Moving region to opened status: {}", regionInfo);

          Fiber tabletFiber = fiberFactory.create();
          c5db.tablet.Tablet tablet = new c5db.tablet.Tablet(server,
              regionInfo,
              tableDescriptor,
              peers,
              null /*basePath*/,
              conf,
              null /*tableFiber*/,
              replicationModule,
              HRegionBridge::new);

          getTabletStateChanges().publish(new TabletStateChange(tablet,
              Tablet.State.Open,
              null));

        } catch (IOException e) {
          LOG.error("Error opening OLogShim for {}, err: {}", regionInfo, e);
//                    getTabletStateChanges().publish(new TabletStateChange(
//                            regionInfo,
//                            null,
//                            0,
//                            e));
        }
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("Unable to open replicator instance for region {}, err: {}",
            regionInfo, t);
//                getTabletStateChanges().publish(new TabletStateChange(
//                        regionInfo,
//                        null,
//                        0,
//                        t));
      }
    }, fiber);
  }

  @Override
  protected void doStop() {
    // TODO close regions.
    this.fiber.dispose();
    notifyStopped();
  }

  private final Channel<TabletStateChange> tabletStateChangeChannel = new MemoryChannel<>();


  @Override
  public void startTablet(List<Long> peers, String tabletName) {

  }

  @Override
  public Channel<TabletStateChange> getTabletStateChanges() {
    return tabletStateChangeChannel;
  }

  @Override
  public ModuleType getModuleType() {
    return ModuleType.Tablet;
  }

  @Override
  public boolean hasPort() {
    return false;
  }

  @Override
  public int port() {
    return 0;
  }

  @Override
  public String acceptCommand(String commandString) {
    return null;
  }

  public int getMinQuorumSize() {
    if (server.isSingleNodeMode()) {
      return 1;
    } else {
      return 3;
    }
  }
}
