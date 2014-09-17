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
import c5db.client.generated.RegionSpecifier;
import c5db.client.generated.TableName;
import c5db.interfaces.C5Module;
import c5db.interfaces.C5Server;
import c5db.interfaces.ControlModule;
import c5db.interfaces.DiscoveryModule;
import c5db.interfaces.ReplicationModule;
import c5db.interfaces.TabletModule;
import c5db.interfaces.discovery.NodeInfo;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.interfaces.tablet.Tablet;
import c5db.interfaces.tablet.TabletStateChange;
import c5db.messages.generated.CommandReply;
import c5db.messages.generated.ModuleSubCommand;
import c5db.messages.generated.ModuleType;
import c5db.regionserver.RegionNotFoundException;
import c5db.tablet.hregionbridge.HRegionBridge;
import c5db.tablet.hregionbridge.HRegionServicesBridge;
import c5db.util.FiberOnly;
import c5db.util.FiberSupplier;
import c5db.util.TabletNameHelpers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
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
import org.jetbrains.annotations.NotNull;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.Request;
import org.jetlang.channels.Session;
import org.jetlang.core.Disposable;
import org.jetlang.fibers.Fiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  private final Configuration conf;
  private final Channel<TabletStateChange> tabletStateChangeChannel = new MemoryChannel<>();
  private ReplicationModule replicationModule = null;
  private DiscoveryModule discoveryModule = null;
  private boolean rootStarted = false;
  protected TabletRegistry tabletRegistry;
  private Disposable newNodeWatcher = null;
  private ControlModule controlModule;

  public TabletService(C5Server server) {
    this.fiberSupplier = server.getFiberSupplier();
    this.fiber = fiberSupplier.getFiber(this::notifyFailed);
    this.server = server;
    this.conf = HBaseConfiguration.create();
  }

  @Override
  protected void doStart() {

    fiber.start();
    fiber.execute(() -> {
      ListenableFuture<C5Module> discoveryService = server.getModule(ModuleType.Discovery);
      ListenableFuture<C5Module> controlService = server.getModule(ModuleType.ControlRpc);

      try {
        discoveryModule = (DiscoveryModule) discoveryService.get();
        controlModule = (ControlModule) controlService.get();
      } catch (InterruptedException | ExecutionException e) {
        notifyFailed(e);
        return;
      }

      ListenableFuture<C5Module> replicatorService = server.getModule(ModuleType.Replication);
      Futures.addCallback(replicatorService, new FutureCallback<C5Module>() {
        @Override
        public void onSuccess(@NotNull C5Module result) {
          replicationModule = (ReplicationModule) result;
          fiber.execute(() -> {
            tabletRegistry = new TabletRegistry(server,
                server.getConfigDirectory(),
                conf,
                getTabletStateChanges(),
                replicationModule,
                ReplicatedTablet::new,
                (basePath, regionInfo, tableDescriptor, log, conf) -> {
                  HRegionServicesBridge hRegionBridge = new HRegionServicesBridge(conf);
                  return new HRegionBridge(HRegion.openHRegion(new org.apache.hadoop.fs.Path(basePath.toString()),
                      regionInfo, tableDescriptor, log, conf, hRegionBridge, null));
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
        public void onFailure(@NotNull Throwable t) {
          notifyFailed(t);
        }
      }, fiber);
    });

  }

  @FiberOnly
  private void startBootstrap() {
    final FutureCallback<ImmutableMap<Long, NodeInfo>> callback =
        new FutureCallback<ImmutableMap<Long, NodeInfo>>() {
          final Map<Long, NodeInfo> peers = new HashMap<>();

          @Override
          @FiberOnly
          public void onSuccess(@NotNull ImmutableMap<Long, NodeInfo> result) {
            peers.putAll(result);
            try {
              maybeStartBootstrap(peers);
            } catch (IOException e) {
              e.printStackTrace();
              System.exit(1);
            }
          }

          @Override
          public void onFailure(@NotNull Throwable t) {
            LOG.warn("failed to get discovery state", t);
          }
        };

    newNodeWatcher = discoveryModule.getNewNodeNotifications().subscribe(fiber, message -> {
      ListenableFuture<ImmutableMap<Long, NodeInfo>> f = discoveryModule.getState();
      Futures.addCallback(f, callback, fiber);
    });
  }

  @FiberOnly
  private void maybeStartBootstrap(Map<Long, NodeInfo> nodes) throws IOException {
    ImmutableList<Long> peers = ImmutableList.copyOf(new ArrayList<>(nodes.keySet()));

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
  private void bootstrapRoot(final ImmutableList<Long> peers) throws IOException {
    HTableDescriptor rootDesc = SystemTableNames.rootTableDescriptor();
    HRegionInfo rootRegion = SystemTableNames.rootRegionInfo();

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
  public Channel<TabletStateChange> getTabletStateChanges() {
    return tabletStateChangeChannel;
  }

  @org.jetbrains.annotations.NotNull
  @Override
  public Tablet getTablet(String tableName, ByteBuffer row) throws RegionNotFoundException {
    return tabletRegistry.getTablet(tableName, row);
  }

  @Override
  public Tablet getTablet(RegionSpecifier regionSpecifier) throws RegionNotFoundException {
    throw new RegionNotFoundException("Not supported");
  }

  @Override
  public Collection<Tablet> getTablets() {
    return this.tabletRegistry.dumpTablets();
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
    try {
      if (commandString.startsWith(C5ServerConstants.START_META)) {
        return startMetaHere(commandString);
      } else if (commandString.startsWith(C5ServerConstants.CREATE_TABLE)) {
        return createUserTable(commandString);
      } else if (commandString.startsWith(C5ServerConstants.LAUNCH_TABLET)) {
        return launchTablet(commandString);
      } else if (commandString.startsWith(C5ServerConstants.SET_META_LEADER)) {
        return setMetaLeader(commandString);
      } else if (commandString.startsWith(C5ServerConstants.SET_USER_LEADER)) {
        return setUserLeader(commandString);
      }
    } catch (IOException | RegionNotFoundException | DeserializationException e) {
      LOG.error(e.getMessage());
      e.printStackTrace();
    }

    LOG.error("Unable to accept command:" + commandString);
    return "NOTOK";
  }

  @Override
  public void startTabletHere(HTableDescriptor hTableDescriptor,
                              HRegionInfo hRegionInfo,
                              ImmutableList<Long> peers) throws IOException {
    this.tabletRegistry.startTablet(hRegionInfo, hTableDescriptor, peers);
  }

  private String launchTablet(String commandString) throws IOException, DeserializationException {
    BASE64Decoder decoder = new BASE64Decoder();
    String createString = commandString.substring(commandString.indexOf(":") + 1);
    String[] tableCreationStrings = createString.split(",");
    HTableDescriptor hTableDescriptor = HTableDescriptor.parseFrom(decoder.decodeBuffer(tableCreationStrings[0]));
    HRegionInfo hRegionInfo = HRegionInfo.parseFrom(decoder.decodeBuffer(tableCreationStrings[1]));
    List<Long> peers = new ArrayList<>();
    for (String s : Arrays.copyOfRange(tableCreationStrings, 2, tableCreationStrings.length)) {
      s = StringUtils.strip(s);
      peers.add(new Long(s));
    }

    startTabletHere(hTableDescriptor, hRegionInfo, ImmutableList.copyOf(peers));
    return "OK";
  }

  private String setMetaLeader(String commandString) throws IOException, RegionNotFoundException {
    int nodeIdOffset = commandString.indexOf(":") + 1;
    String nodeId = commandString.substring(nodeIdOffset);
    addMetaLeaderEntryToRoot(Long.parseLong(nodeId));
    return "OK";
  }

  private String setUserLeader(String commandString)
      throws IOException, RegionNotFoundException, DeserializationException {
    BASE64Decoder decoder = new BASE64Decoder();
    String createString = commandString.substring(commandString.indexOf(":") + 1);
    String[] splits = createString.split(",");
    addLeaderEntryToMeta(Long.parseLong(splits[0]), HRegionInfo.parseFrom(decoder.decodeBuffer(splits[1])));
    return "OK";
  }


  private String createUserTable(String commandString)
      throws IOException, DeserializationException, RegionNotFoundException {
    BASE64Decoder decoder = new BASE64Decoder();
    String createString = commandString.substring(commandString.indexOf(":") + 1);
    String[] tableCreationStrings = createString.split(",");

    HTableDescriptor hTableDescriptor;
    ArrayList<HRegionInfo> hRegionInfos = new ArrayList<>();
    List<Long> peers = new ArrayList<>();

    hTableDescriptor = HTableDescriptor.parseFrom(decoder.decodeBuffer(tableCreationStrings[0]));
    hRegionInfos.add(HRegionInfo.parseFrom(decoder.decodeBuffer(tableCreationStrings[1])));

    String[] extraHRegionStrings = Arrays.copyOfRange(tableCreationStrings, 2, tableCreationStrings.length - 1);
    for (String s : extraHRegionStrings) {
      try {
        byte[] potentialHRegion = decoder.decodeBuffer(s);
        HRegionInfo hRegion = HRegionInfo.parseFrom(potentialHRegion);
        hRegionInfos.add(hRegion);
      } catch (DeserializationException e) {
        // WE have gotten to the peer list
        break;
      }
    }

    for (String s : Arrays.copyOfRange(tableCreationStrings, hRegionInfos.size() + 1, tableCreationStrings.length)) {
      s = StringUtils.strip(s);
      peers.add(new Long(s));
    }

    for (HRegionInfo hRegionInfo : hRegionInfos) {
      notifyCohortsForTabletCreation(peers, hTableDescriptor, hRegionInfo, 3);
    }
    for (HRegionInfo hRegionInfo : hRegionInfos) {
      addEntryToMeta(hRegionInfo, hTableDescriptor);
    }
    return "OK";
  }

  private void notifyCohortsForTabletCreation(final List<Long> peers,
                                              final HTableDescriptor hTableDescriptor,
                                              final HRegionInfo hRegionInfo,
                                              final int maximumNumberOfCohorts) {
    int numberOfCohorts = peers.size() < 3 ? peers.size() : maximumNumberOfCohorts;
    ArrayList<Long> shuffledPeers = new ArrayList<>(peers);
    Collections.shuffle(shuffledPeers);
    List<Long> subList = shuffledPeers.subList(0, numberOfCohorts);
    for (long peer : subList) {
      ModuleSubCommand moduleSubCommand
          = prepareTabletModuleSubCommand(prepareLaunchTabletString(hTableDescriptor, hRegionInfo, subList));
      relayRequest(prepareRequest(peer, moduleSubCommand));
    }
  }

  private String startMetaHere(String commandString)
      throws IOException {
    HTableDescriptor metaDesc = HTableDescriptor.META_TABLEDESC;
    HRegionInfo metaRegion = SystemTableNames.metaRegionInfo();
    // ok we have enough to start a region up now:
    String peerString = commandString.substring(commandString.indexOf(":") + 1);
    List<Long> peers = new ArrayList<>();
    for (String s : peerString.split(",")) {
      peers.add(new Long(s));
    }

    this.startTabletHere(metaDesc, metaRegion, ImmutableList.copyOf(peers));
    return "OK";
  }

  private ModuleSubCommand prepareTabletModuleSubCommand(String command) {
    return new ModuleSubCommand(ModuleType.Tablet, command);
  }

  private Request<CommandRpcRequest<?>, CommandReply>
  prepareRequest(long peer, ModuleSubCommand moduleSubCommand) {
    CommandRpcRequest<ModuleSubCommand> commandRpcRequest = new CommandRpcRequest<>(peer, moduleSubCommand);
    return new Request<CommandRpcRequest<?>, CommandReply>() {

      @Override
      public Session getSession() {
        return null;
      }

      @Override
      public CommandRpcRequest<?> getRequest() {
        return commandRpcRequest;
      }

      @Override
      public void reply(CommandReply i) {
      }
    };
  }

  private void relayRequest(Request<CommandRpcRequest<?>, CommandReply> request) {
    controlModule.doMessage(request);
  }

  private static String prepareLaunchTabletString(HTableDescriptor hTableDescriptor,
                                                  HRegionInfo hRegionInfo,
                                                  List<Long> peers) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(C5ServerConstants.LAUNCH_TABLET);
    stringBuilder.append(":");
    BASE64Encoder encoder = new BASE64Encoder();
    stringBuilder.append(encoder.encode(hTableDescriptor.toByteArray()));
    stringBuilder.append(",");
    stringBuilder.append(encoder.encode(hRegionInfo.toByteArray()));

    for (Long peer : peers) {
      stringBuilder.append(",");
      stringBuilder.append(peer);
    }

    return stringBuilder.toString();
  }

  private void addMetaLeaderEntryToRoot(long leader) throws IOException, RegionNotFoundException {
    Tablet tablet = this.tabletRegistry.getTablet("hbase:root", new byte[]{0x00});
    org.apache.hadoop.hbase.TableName hbaseDatabaseName = SystemTableNames.metaTableName();
    ByteBuffer hbaseNameSpace = ByteBuffer.wrap(hbaseDatabaseName.getNamespace());
    ByteBuffer hbaseTableName = ByteBuffer.wrap(hbaseDatabaseName.getQualifier());
    TableName tableName = new TableName(hbaseNameSpace, hbaseTableName);

    if (tablet.getLeader() != server.getNodeId()) {
      throw new IOException(" I am not leader but I am trying to put into root");
    }
    Put put = new Put(TabletNameHelpers.toBytes(tableName));
    put.add(HConstants.CATALOG_FAMILY, C5ServerConstants.LEADER_QUALIFIER, Bytes.toBytes(leader));
    MutationProto mutation = ProtobufUtil.toMutation(MutationProto.MutationType.PUT, put);
    boolean processed = tablet.getRegion().mutate(mutation, new Condition());
    if (!processed) {
      throw new IOException("Unable to process root change");
    }
  }

  private void addEntryToMeta(HRegionInfo hRegionInfo, HTableDescriptor hTableDescriptor)
      throws IOException, RegionNotFoundException {
    byte[] tableName = hRegionInfo.getTable().getName();
    byte[] tableNameAndRow;
    if (hRegionInfo.getEndKey() == null || hRegionInfo.getEndKey().length == 0) {
      tableNameAndRow = Bytes.add(tableName, SystemTableNames.sep, new byte[0]);
    } else {
      tableNameAndRow = Bytes.add(tableName, SystemTableNames.sep, hRegionInfo.getEndKey());
    }

    byte[] metaRowKey = Bytes.add(tableNameAndRow, SystemTableNames.sep, Bytes.toBytes(hRegionInfo.getRegionId()));
    Tablet tablet = this.tabletRegistry.getTablet("hbase:meta", metaRowKey);
    if (tablet.getLeader() != server.getNodeId()) {
     throw new IOException(" I am not leader but I am trying to put into meta");
    }
    Put put = new Put(hRegionInfo.getRegionName());
    put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER, hRegionInfo.toByteArray());
    put.add(HConstants.CATALOG_FAMILY, HTABLE_DESCRIPTOR_QUALIFIER, hTableDescriptor.toByteArray());
    MutationProto mutation = ProtobufUtil.toMutation(MutationProto.MutationType.PUT, put);
    boolean processed = tablet.getRegion().mutate(mutation, new Condition());
    if (!processed) {
      throw new IOException("Unable to process root change");
    }

  }


  private void addLeaderEntryToMeta(long leader, HRegionInfo hRegionInfo) throws IOException, RegionNotFoundException {
    Tablet tablet = this.tabletRegistry.getTablet("hbase:meta", new byte[]{0x00});
    if (tablet.getLeader() == server.getNodeId()) {
      Put put = new Put(hRegionInfo.getRegionName());

      put.add(HConstants.CATALOG_FAMILY, C5ServerConstants.LEADER_QUALIFIER, Bytes.toBytes(leader));
      tablet.getRegion().mutate(ProtobufUtil.toMutation(MutationProto.MutationType.PUT, put), new Condition());
    } else {
      throw new IOException("We are not meta, but we got the command to start it ");
    }
  }

  int getMinQuorumSize() {
    if (server.isSingleNodeMode()) {
      return 1;
    } else {
      return server.getMinQuorumSize();
    }
  }
}
