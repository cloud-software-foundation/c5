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
import c5db.client.ProtobufUtil;
import c5db.client.generated.Condition;
import c5db.client.generated.MutationProto;
import c5db.interfaces.C5Module;
import c5db.interfaces.C5Server;
import c5db.interfaces.DiscoveryModule;
import c5db.interfaces.ReplicationModule;
import c5db.interfaces.TabletModule;
import c5db.interfaces.discovery.NodeInfo;
import c5db.interfaces.tablet.Tablet;
import c5db.interfaces.tablet.TabletStateChange;
import c5db.messages.generated.ModuleType;
import c5db.regionserver.RegionNotFoundException;
import c5db.tablet.hregionbridge.HRegionBridge;
import c5db.tablet.hregionbridge.HRegionServicesBridge;
import c5db.util.FiberOnly;
import c5db.util.FiberSupplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.core.Disposable;
import org.jetlang.fibers.Fiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Decoder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;


/**
 * The main entry point for the service which manages the tablet level lifecycle.
 * The only place Tablets are created.
 */
public class TabletService extends AbstractService implements TabletModule {
  private static final Logger LOG = LoggerFactory.getLogger(TabletService.class);
  private static final byte[] HTABLE_DESCRIPTOR_QUALIFIER = Bytes.toBytes("HTABLE_QUAL");

  private final FiberSupplier fiberSupplier;
  private final Fiber fiber;
  private final C5Server server;
  // TODO bring this into this class, and not have an external class.

  private final Configuration conf;
  private final Channel<TabletStateChange> tabletStateChangeChannel = new MemoryChannel<>();
  private ReplicationModule replicationModule = null;
  private DiscoveryModule discoveryModule = null;
  private boolean rootStarted = false;
  protected TabletRegistry tabletRegistry;
  private Disposable newNodeWatcher = null;

  public TabletService(C5Server server) {
    this.fiberSupplier = server.getFiberSupplier();
    this.fiber = fiberSupplier.getFiber(this::notifyFailed);
    this.server = server;
    this.conf = HBaseConfiguration.create();

  }

  @Override
  public Tablet getTablet(String tabletName) throws RegionNotFoundException {
    Tablet tablet = getRegionWithJustTableName(tabletName);
    if (tablet != null) {
      return tablet;
    } else {
      throw new RegionNotFoundException("Unable to find specified tablet:" + tabletName);
    }
  }

  // TODO remove
  private Tablet getRegionWithJustTableName(String tableName) {
    // Always return the first region which matches
    if (tabletRegistry == null) {
      return null;
    }
    Optional<String> maybeFoundRegion = tabletRegistry
        .getTablets()
        .keySet()
        .stream()
        .filter(s -> s.startsWith(tableName))
        .findFirst();
    if (maybeFoundRegion.isPresent()) {
      return tabletRegistry.getTablets().get(maybeFoundRegion.get());
    } else {
      LOG.error("Region not found: " + tableName);
      return null;
    }
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
            tabletRegistry = new TabletRegistry(server,
                server.getConfigDirectory(),
                conf,
                getTabletStateChanges(),
                replicationModule,
                ReplicatedTablet::new,
                (basePath, regionInfo, tableDescriptor, log, conf) -> {
                  HRegion hregion = null;
                  HRegionServicesBridge hRegionBridge = new HRegionServicesBridge(conf);
                    hregion = HRegion.openHRegion(new org.apache.hadoop.fs.Path(basePath.toString()),
                        regionInfo,
                        tableDescriptor,
                        log,
                        conf,
                        hRegionBridge,
                        null);
                  return new HRegionBridge(hregion);
                }
            );
            try {
              startBootstrap();
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

  @FiberOnly
  private void startBootstrap() {
    LOG.info("Waiting to find at least " + getMinQuorumSize() + " nodes to bootstrap with");

    final FutureCallback<ImmutableMap<Long, NodeInfo>> callback =
        new FutureCallback<ImmutableMap<Long, NodeInfo>>() {
          @Override
          @FiberOnly
          public void onSuccess(ImmutableMap<Long, NodeInfo> result) {
            try {
              maybeStartBootstrap(result);
            } catch (IOException e) {
              e.printStackTrace();
            }
          }

          @Override
          public void onFailure(Throwable t) {
            LOG.warn("failed to get discovery state", t);
          }
        };

    newNodeWatcher = discoveryModule.getNewNodeNotifications().subscribe(fiber, message -> {
      ListenableFuture<ImmutableMap<Long, NodeInfo>> f = discoveryModule.getState();
      Futures.addCallback(f, callback, fiber);
    });

    ListenableFuture<ImmutableMap<Long, NodeInfo>> f = discoveryModule.getState();
    Futures.addCallback(f, callback, fiber);
  }

  @FiberOnly
  private void maybeStartBootstrap(ImmutableMap<Long, NodeInfo> nodes) throws IOException {
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
    if (newNodeWatcher != null) {
      newNodeWatcher.dispose();
      newNodeWatcher = null;
    }
  }

  // to bootstrap root we need to find the list of peers we should be connected to, and then do that.
  // how to bootstrap?
  private void bootstrapRoot(final List<Long> peers) throws IOException {
    HTableDescriptor rootDesc = HTableDescriptor.ROOT_TABLEDESC;
    HRegionInfo rootRegion = new HRegionInfo(
        rootDesc.getTableName(), new byte[]{0}, new byte[]{}, false, 1);

    // ok we have enough to start a region up now:

    tabletRegistry.startTablet(rootRegion, rootDesc, peers);
  }

  @Override
  protected void doStop() {
    // TODO close regions.
    this.fiber.dispose();
    notifyStopped();
  }

  @Override
  public void startTablet(List<Long> peers, String tabletName) {
    LOG.error("unimplemented");
    System.exit(1);
  }

  @Override
  public Channel<TabletStateChange> getTabletStateChanges() {
    return tabletStateChangeChannel;
  }

  @Override
  public Collection<Tablet> getTablets() throws ExecutionException, InterruptedException {
    SettableFuture<Collection<Tablet>> future = SettableFuture.create();
    fiber.execute(() -> {
      Map<String, Tablet> tablets = tabletRegistry.getTablets();
      // make defensive copy:
      future.set(
          Lists.newArrayList(tablets.values())
      );
    });

    return future.get();
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
  public String acceptCommand(String commandString) throws InterruptedException {
    try {
      if (commandString.startsWith(C5ServerConstants.START_META)) {
        return startMeta(commandString);
      } else if (commandString.startsWith(C5ServerConstants.CREATE_TABLE)) {
        return createUserTable(commandString);
      } else if (commandString.startsWith(C5ServerConstants.SET_META_LEADER)) {
        return setMetaLeader(commandString);
      } else {
        throw new IOException("Unknown command:" + commandString);
      }
    } catch (IOException | RegionNotFoundException | DeserializationException e) {
      e.printStackTrace();
    }

    return "NOTOK";
  }

  private String setMetaLeader(String commandString) throws IOException, RegionNotFoundException {
    int nodeIdOffset = commandString.indexOf(":") + 1;
    String nodeId = commandString.substring(nodeIdOffset);
    addMetaLeaderEntryToRoot(Long.parseLong(nodeId));
    return "OK";
  }

  private String createUserTable(String commandString)
      throws IOException, DeserializationException, RegionNotFoundException {
    BASE64Decoder decoder = new BASE64Decoder();
    String createString = commandString.substring(commandString.indexOf(":") + 1);
    String[] tableCreationStrings = createString.split(",");

    HTableDescriptor hTableDescriptor;
    HRegionInfo hRegionInfo;
    List<Long> peers = new ArrayList<>();

    for (String s : Arrays.copyOfRange(tableCreationStrings, 2, tableCreationStrings.length)) {
      s = StringUtils.strip(s);
      peers.add(new Long(s));
    }
    hTableDescriptor = HTableDescriptor.parseFrom(decoder.decodeBuffer(tableCreationStrings[0]));
    hRegionInfo = HRegionInfo.parseFrom(decoder.decodeBuffer(tableCreationStrings[1]));
    return createUserTableHelper(peers, hTableDescriptor, hRegionInfo);
  }

  private String startMeta(String commandString) throws IOException {
    HTableDescriptor metaDesc = HTableDescriptor.META_TABLEDESC;
    HRegionInfo metaRegion = SystemTableNames.metaRegionInfo();
    // ok we have enough to start a region up now:
    String peerString = commandString.substring(commandString.indexOf(":") + 1);
    List<Long> peers = new ArrayList<>();
    for (String s : peerString.split(",")) {
      peers.add(new Long(s));
    }
    tabletRegistry.startTablet(metaRegion, metaDesc, peers);
    return "OK";
  }

  private String createUserTableHelper(List<Long> peers,
                                       HTableDescriptor hTableDescriptor,
                                       HRegionInfo hRegionInfo) throws IOException, RegionNotFoundException {
    addEntryToMeta(hRegionInfo, hTableDescriptor);
    tabletRegistry.startTablet(hRegionInfo, hTableDescriptor, peers);
    return "OK";
  }

  private void addEntryToMeta(HRegionInfo hRegionInfo, HTableDescriptor hTableDescriptor)
      throws IOException, RegionNotFoundException {
    Region region = this.getTablet("hbase:meta").getRegion();

    Put put = new Put(hRegionInfo.getEncodedNameAsBytes());
    put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER, hRegionInfo.toByteArray());
    put.add(HConstants.CATALOG_FAMILY, HTABLE_DESCRIPTOR_QUALIFIER, hTableDescriptor.toByteArray());


    region.mutate(ProtobufUtil.toMutation(MutationProto.MutationType.PUT, put), new Condition());
  }

  private void addMetaLeaderEntryToRoot(long leader) throws IOException, RegionNotFoundException {
    Region region = this.getTablet("hbase:root").getRegion();
    HRegionInfo hRegionInfo = SystemTableNames.rootRegionInfo();
    Put put = new Put(hRegionInfo.getEncodedNameAsBytes());
    put.add(HConstants.CATALOG_FAMILY, C5ServerConstants.LEADER_QUALIFIER, Bytes.toBytes(leader));
    region.mutate(ProtobufUtil.toMutation(MutationProto.MutationType.PUT, put), new Condition());
  }

  private void addLeaderEntryToMeta(long leader) throws IOException, RegionNotFoundException {
    Region region = this.getTablet("hbase:meta").getRegion();
    HRegionInfo hRegionInfo = SystemTableNames.metaRegionInfo();
    Put put = new Put(hRegionInfo.getEncodedNameAsBytes());

    put.add(HConstants.CATALOG_FAMILY, C5ServerConstants.LEADER_QUALIFIER, Bytes.toBytes(leader));
    region.mutate(ProtobufUtil.toMutation(MutationProto.MutationType.PUT, put), new Condition());
  }

  int getMinQuorumSize() {
    if (server.isSingleNodeMode()) {
      return 1;
    } else {
      return server.getMinQuorumSize();
    }
  }
}