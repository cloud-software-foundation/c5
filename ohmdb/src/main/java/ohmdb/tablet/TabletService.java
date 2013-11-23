/*
 * Copyright (C) 2013  Ohm Data
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
package ohmdb.tablet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import ohmdb.client.OhmConstants;
import ohmdb.generated.Log;
import ohmdb.interfaces.DiscoveryModule;
import ohmdb.interfaces.OhmModule;
import ohmdb.interfaces.OhmServer;
import ohmdb.interfaces.ReplicationModule;
import ohmdb.interfaces.TabletModule;
import ohmdb.log.OLogShim;
import ohmdb.regionserver.RegistryFile;
import ohmdb.util.FiberOnly;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.core.Callback;
import org.jetlang.core.Disposable;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static ohmdb.messages.generated.ControlMessages.ModuleType;

/**
 *
 */
public class TabletService extends AbstractService implements TabletModule {
    private static final Logger LOG = LoggerFactory.getLogger(TabletService.class);

    private final PoolFiberFactory fiberFactory;
    private final Fiber fiber;
    private final OhmServer server;
    // TODO bring this into this class, and not have an external class.
    //private final OnlineRegions onlineRegions = OnlineRegions.INSTANCE;
    private final Map<String, HRegion> onlineRegions = new HashMap<>();
    private ReplicationModule replicationModule = null;
    private DiscoveryModule discoveryModule = null;
    private final Configuration conf;

    public TabletService(PoolFiberFactory fiberFactory, OhmServer server) {
        this.fiberFactory = fiberFactory;
        this.fiber = fiberFactory.create();
        this.server = server;
        this.conf = HBaseConfiguration.create();

    }

    @Override
    public HRegion getTablet(String tabletName) {
        // TODO ugly hack fix eventually
        return onlineRegions.values().iterator().next();
    }

    @Override
    protected void doStart() {
        fiber.start();

        fiber.execute(new Runnable() {
            @Override
            public void run() {

                ListenableFuture<OhmModule> discoveryService = server.getModule(ModuleType.Discovery);
                try {
                    discoveryModule = (DiscoveryModule) discoveryService.get();
                } catch (InterruptedException | ExecutionException e) {
                    notifyFailed(e);
                    return;
                }

                ListenableFuture<OhmModule> replicatorService = server.getModule(ModuleType.Replication);
                Futures.addCallback(replicatorService, new FutureCallback<OhmModule>() {
                    @Override
                    public void onSuccess(OhmModule result) {
                        replicationModule = (ReplicationModule) result;
                        fiber.execute(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    Path path = server.getConfigDirectory().baseConfigPath;


                                    RegistryFile registryFile = new RegistryFile(path);

                                    int startCount = startRegions(registryFile);

                                    // if no regions were started, we need to bootstrap once we have
                                    // enough online regions.
                                    if (startCount == 0) {
                                        startBootstrap(registryFile);
                                    }

                                    logReplay(path);

                                    notifyStarted();
                                } catch (Exception e) {
                                    notifyFailed(e);
                                }
                            }
                        });
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        notifyFailed(t);
                    }
                }, fiber);
            }
        });

    }

    Disposable newNodeWatcher = null;


    @FiberOnly
    private void startBootstrap(final RegistryFile registryFile) throws IOException {
        LOG.info("Waiting to find at least 3 nodes to bootstrap with");

        final FutureCallback<ImmutableMap<Long, DiscoveryModule.NodeInfo>> callback = new FutureCallback<ImmutableMap<Long, DiscoveryModule.NodeInfo>>() {
            @Override
            public void onSuccess(ImmutableMap<Long, DiscoveryModule.NodeInfo> result) {
                maybeStartBootstrap(registryFile, result);
            }

            @Override
            public void onFailure(Throwable t) {
                LOG.warn("failed to get discovery state", t);
            }
        };

        newNodeWatcher = discoveryModule.getNewNodeNotifications().subscribe(fiber, new Callback<DiscoveryModule.NewNodeVisible>() {
            @Override
            public void onMessage(DiscoveryModule.NewNodeVisible message) {
                ListenableFuture<ImmutableMap<Long, DiscoveryModule.NodeInfo>> f = discoveryModule.getState();
                Futures.addCallback(f, callback, fiber);
            }
        });

        ListenableFuture<ImmutableMap<Long, DiscoveryModule.NodeInfo>> f = discoveryModule.getState();
        Futures.addCallback(f, callback, fiber);
    }

    private void maybeStartBootstrap(RegistryFile registryFile, ImmutableMap<Long, DiscoveryModule.NodeInfo> nodes) {
        List<Long> peers = new ArrayList<>(nodes.keySet());

        LOG.debug("Found a bunch of peers: {}", peers);
        if (peers.size() < 3)
            return;

        // bootstrap the frickin thing.
        LOG.debug("Bootstrapping empty region");
        // simple bootstrap, only bootstrap my own ID:
        byte[] startKey = {0};
        byte[] endKey = {};
        TableName tableName = TableName.valueOf("tableName");
        HRegionInfo hRegionInfo = new HRegionInfo(tableName,
                startKey, endKey, false, 0);
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        tableDescriptor.addFamily(new HColumnDescriptor("cf"));

        try {
            registryFile.addEntry(hRegionInfo, new HColumnDescriptor("cf"), peers);
        } catch (IOException e) {
            LOG.error("Cant append to registryFile, not bootstrapping!!!", e);
            return;
        }

        openRegion0(hRegionInfo, tableDescriptor, ImmutableList.copyOf(peers));

        if (newNodeWatcher != null) {
            newNodeWatcher.dispose();
            newNodeWatcher = null;
        }
    }


    @FiberOnly
    private int startRegions(RegistryFile registryFile) throws IOException {
        RegistryFile.Registry registry = registryFile.getRegistry();
        int cnt = 0;
        for (HRegionInfo regionInfo : registry.regions.keySet()) {
            HTableDescriptor tableDescriptor =
                    new HTableDescriptor(regionInfo.getTableName());
            for (HColumnDescriptor cf : registry.regions.get(regionInfo)) {
                tableDescriptor.addFamily(cf);
            }
            // we have a table now.
            ImmutableList<Long> peers = registry.peers.get(regionInfo);

            // open a region async.
            openRegion0(regionInfo, tableDescriptor, peers);
            cnt++;
        }
        return cnt;
    }

    private void openRegion0(final HRegionInfo regionInfo,
                             final HTableDescriptor tableDescriptor,
                             final ImmutableList<Long> peers) {
        LOG.debug("Opening replicator for region {} peers {}", regionInfo, peers);

        ListenableFuture<ReplicationModule.Replicator> future =
                replicationModule.createReplicator(regionInfo.getRegionNameAsString(), peers);
        Futures.addCallback(future, new FutureCallback<ReplicationModule.Replicator>() {
            @Override
            public void onSuccess(ReplicationModule.Replicator result) {
                try {
                    // TODO subscribe to the replicator's broadcasts.

                    result.start();
                    OLogShim shim = new OLogShim(result);

                    // default place for a region is....
                    // tableName/encodedName.
                    HRegion region = HRegion.openHRegion(new org.apache.hadoop.fs.Path(server.getConfigDirectory().baseConfigPath.toString()),
                            regionInfo,
                            tableDescriptor,
                            shim,
                            conf,
                            null, null);

                    onlineRegions.put(regionInfo.getRegionNameAsString(), region);

                    LOG.debug("Moving region to opened status: {}", regionInfo);
                    getTabletStateChanges().publish(new TabletStateChange(regionInfo,
                            region,
                            1, null));

                } catch (IOException e) {
                    LOG.error("Error opening OLogShim for {}, err: {}", regionInfo, e);
                    getTabletStateChanges().publish(new TabletStateChange(
                            regionInfo,
                            null,
                            0,
                            e));
                }
            }

            @Override
            public void onFailure(Throwable t) {
                LOG.error("Unable to open replicator instance for region {}, err: {}",
                        regionInfo, t);
                getTabletStateChanges().publish(new TabletStateChange(
                        regionInfo,
                        null,
                        0,
                        t));
            }
        }, fiber);
    }

    private void logReplay(final Path path) throws IOException {
        java.nio.file.Path archiveLogPath = Paths.get(path.toString(),
                OhmConstants.ARCHIVE_DIR);
        File[] archiveLogs = archiveLogPath.toFile().listFiles();

        if (archiveLogs == null) {
            return;
        }

        for (File log : archiveLogs) {
            FileInputStream rif = new FileInputStream(log);
            processLogFile(rif);
            for (HRegion r : onlineRegions.values()) {
                r.flushcache();
            }
        }
        for (HRegion r : onlineRegions.values()) {
            r.compactStores();
        }

        for (HRegion r : onlineRegions.values()) {
            r.waitForFlushesAndCompactions();
        }

        //TODO WE SHOULDN"T BE ONLINE TIL THIS HAPPENS
    }

    private void processLogFile(FileInputStream rif) throws IOException {
        Log.OLogEntry entry;
        Log.Entry edit;
        do {
            entry = Log.OLogEntry.parseDelimitedFrom(rif);
            // if ! at EOF                      z
            if (entry != null) {
                edit = Log.Entry.parseFrom(entry.getValue());
                HRegion recoveryRegion = onlineRegions.get(edit.getRegionInfo());

                if (recoveryRegion.getLastFlushTime() >= edit.getTs()) {
                    Put put = new Put(edit.getKey().toByteArray());
                    put.add(edit.getFamily().toByteArray(),
                            edit.getColumn().toByteArray(),
                            edit.getTs(),
                            edit.getValue().toByteArray());
                    put.setDurability(Durability.SKIP_WAL);
                    recoveryRegion.put(put);
                }
            }
        } while (entry != null);
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
}
