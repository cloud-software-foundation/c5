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

package c5db.regionserver;

import c5db.C5ServerConstants;
import c5db.client.ExplicitNodeCaller;
import c5db.client.FakeHTable;
import c5db.client.ProtobufUtil;
import c5db.client.generated.Location;
import c5db.client.generated.RegionLocation;
import c5db.client.generated.RegionSpecifier;
import c5db.client.generated.Result;
import c5db.client.generated.ScanRequest;
import c5db.client.generated.TableName;
import c5db.client.scanner.C5ClientScanner;
import c5db.codec.websocket.Initializer;
import c5db.control.SimpleControlClient;
import c5db.interfaces.C5Module;
import c5db.interfaces.C5Server;
import c5db.interfaces.DiscoveryModule;
import c5db.interfaces.RegionServerModule;
import c5db.interfaces.TabletModule;
import c5db.interfaces.discovery.NodeInfoReply;
import c5db.interfaces.tablet.Tablet;
import c5db.messages.generated.CommandReply;
import c5db.messages.generated.ModuleType;
import c5db.tablet.Region;
import c5db.tablet.SystemTableNames;
import c5db.tablet.TabletRegistry;
import c5db.util.C5FiberFactory;
import c5db.util.C5Futures;
import c5db.util.TabletNameHelpers;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.jetty.util.MultiException;
import org.jetbrains.annotations.NotNull;
import org.jetlang.fibers.Fiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The service handler for the RegionServer class. Responsible for handling the internal lifecycle
 * and attaching the netty infrastructure to the region server.
 */
public class RegionServerService extends AbstractService implements RegionServerModule {
  private static final Logger LOG = LoggerFactory.getLogger(RegionServerService.class);
  private final ScannerManager scanManager = new ScannerManager();
  private final Fiber fiber;
  private final EventLoopGroup acceptGroup;
  private final EventLoopGroup workerGroup;
  private final int port;
  private final C5Server server;
  private final ServerBootstrap bootstrap = new ServerBootstrap();
  private TabletModule tabletModule;
  private Channel listenChannel;
  public final AtomicLong scannerCounter;
  private final Location location;
  private DiscoveryModule discoveryModule;

  private ConcurrentSkipListMap<HRegionInfo, Long> metaCache = new ConcurrentSkipListMap<>(new MetaCacheComparable());
  public RegionServerService(EventLoopGroup acceptGroup,
                             EventLoopGroup workerGroup,
                             int port,
                             C5Server server) throws UnknownHostException {
    this.acceptGroup = acceptGroup;
    this.workerGroup = workerGroup;
    this.port = port;
    this.server = server;
    C5FiberFactory fiberFactory = server.getFiberFactory(this::notifyFailed);
    this.fiber = fiberFactory.create();
//    bootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
    this.scannerCounter = new AtomicLong(0);
    this.location = new Location(InetAddress.getLocalHost().getHostName(), port);
  }

  @Override
  protected void doStart() {
    fiber.start();

    fiber.execute(() -> {
      // we need the tablet module:
      ListenableFuture<C5Module> f = server.getModule(ModuleType.Tablet);
      allowWebsocketHandlers(f);

      ListenableFuture<C5Module> discoveryFuture = server.getModule(ModuleType.Discovery);
      try {
        this.discoveryModule = (DiscoveryModule) discoveryFuture.get();
      } catch (InterruptedException | ExecutionException e) {
        notifyFailed(e);
      }

    });
  }

  private void allowWebsocketHandlers(ListenableFuture<C5Module> f) {
    Futures.addCallback(f, new FutureCallback<C5Module>() {
      @Override
      public void onSuccess(@NotNull final C5Module result) {
        tabletModule = (TabletModule) result;
        bootstrap.group(acceptGroup, workerGroup)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .channel(NioServerSocketChannel.class)
            .childHandler(new Initializer(RegionServerService.this));

        bootstrap.bind(port).addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
              listenChannel = future.channel();
              notifyStarted();
            } else {
              LOG.error("Unable to find Region Server to {} {}", port, future.cause());
              notifyFailed(future.cause());
            }
          }
        });
      }

      @Override
      public void onFailure(@NotNull Throwable t) {
        notifyFailed(t);
      }
    }, fiber);
  }

  @Override
  protected void doStop() {
    try {
      listenChannel.close().get();
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
      notifyFailed(e);
    }

    notifyStopped();
  }

  @Override
  public ModuleType getModuleType() {
    return ModuleType.RegionServer;
  }

  @Override
  public boolean hasPort() {
    return true;
  }

  @Override
  public int port() {
    return port;
  }

  @Override
  public String acceptCommand(final String commandString) {
    return null;
  }

  public String toString() {

    return super.toString() + '{' + "port = " + port + '}';
  }

  Fiber getNewFiber() throws Exception {
    List<Throwable> throwables = new ArrayList<>();
    C5FiberFactory ff = server.getFiberFactory(throwables::add);
    if (throwables.size() > 0) {
      MultiException exception = new MultiException();
      throwables.forEach(exception::add);
      throw exception;
    }
    return ff.create();
  }

  public ScannerManager getScanManager() {
    return scanManager;
  }

  public long leaderResultFromMeta(TableName tableName, ByteBuffer row)
      throws InterruptedException, ExecutionException, TimeoutException, IOException, RegionNotFoundException, URISyntaxException, DeserializationException {

    org.apache.hadoop.hbase.TableName hbaseTableName = TabletNameHelpers.getHBaseTableName(tableName);
    byte[] metaScanningKey = Bytes.add(TabletNameHelpers.toBytes(tableName), Bytes.toBytes(","), row.array());
    HRegionInfo fakeHRegionInfo = new HRegionInfo(hbaseTableName, metaScanningKey, metaScanningKey);
    long leader;
    if (!metaCache.containsKey(fakeHRegionInfo)){
      Location metaLocation = getMetaLocation(tableName, row);
      TableName metaTableName = TabletNameHelpers.getClientTableName(SystemTableNames.metaTableName());
      FakeHTable metaTable = new FakeHTable(metaLocation.getHostname(), metaLocation.getPort(), metaTableName);
      Scan scan = new Scan(Bytes.add(metaScanningKey, Bytes.toBytes(","), Bytes.toBytes(Long.MAX_VALUE)));
      scan.addFamily(HConstants.CATALOG_FAMILY);
      ResultScanner scanner = metaTable.getScanner(scan);
      org.apache.hadoop.hbase.client.Result result = scanner.next();
      Cell leaderCell = result.getColumnLatestCell(HConstants.CATALOG_FAMILY, C5ServerConstants.LEADER_QUALIFIER);
      Cell regionInfoCell = result.getColumnLatestCell(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
      HRegionInfo regionInfo = HRegionInfo.parseFrom(regionInfoCell.getValue());
      leader = Bytes.toLong(leaderCell.getValue());
      metaCache.put(regionInfo, leader );
    } else {
      leader = metaCache.get(fakeHRegionInfo);
    }
    return leader;
  }

  private Location getMetaLocation(TableName tableName, ByteBuffer row)
      throws RegionNotFoundException, IOException, ExecutionException, InterruptedException {
    NodeInfoReply nodeInfo = null;

    while (nodeInfo == null || !nodeInfo.found || nodeInfo.addresses == null || nodeInfo.addresses.size() == 0) {
      Tablet tablet = this.getLocallyLeaderedTablet(TabletNameHelpers.getClientTableName("hbase", "root"), row);
      Scan scan = new Scan();
      scan.addColumn(HConstants.CATALOG_FAMILY, C5ServerConstants.LEADER_QUALIFIER);
      RegionScanner scanner = tablet.getRegion().getScanner(ProtobufUtil.toScan(scan));
      ArrayList<Cell> results = new ArrayList<>();
      scanner.next(results);
      long leader = Bytes.toLong(results.iterator().next().getValue());
      nodeInfo = discoveryModule.getNodeInfo(leader, ModuleType.RegionServer).get();
    }
    return new Location(nodeInfo.addresses.get(0), nodeInfo.port);
  }

  public Tablet getLocallyLeaderedTablet(TableName tableName, ByteBuffer row) throws RegionNotFoundException {
    return tabletModule.getTablet(TabletNameHelpers.toString(tableName), row);
  }

  public Region getRegion(RegionSpecifier regionSpecifier) throws RegionNotFoundException {
    return this.tabletModule.getTablet(regionSpecifier).getRegion();
  }

  public RegionLocation getNextLocalTabletLocation(Tablet tablet) throws RegionNotFoundException {
    if (tablet.getRegionInfo().getEndKey() == null || tablet.getRegionInfo().getEndKey().length == 0){
      return null;
    }
    HRegionInfo hregionInfo = tablet.getRegionInfo();
    org.apache.hadoop.hbase.TableName hbaseTableName = hregionInfo.getTable();
    TableName clientTableName = TabletNameHelpers.getClientTableName(hbaseTableName);
    Tablet newTablet = getLocallyLeaderedTablet(clientTableName, ByteBuffer.wrap(hregionInfo.getEndKey()));

    hregionInfo = newTablet.getRegionInfo();

    return new RegionLocation(TabletNameHelpers.getClientTableName(hregionInfo.getTable()),
        location,
        ByteBuffer.wrap(hregionInfo.getStartKey()),
        ByteBuffer.wrap(hregionInfo.getEndKey()),
        hregionInfo.getRegionId());
  }

  public C5Server getServer() {
    return server;
  }

  public class ScannerManager {
    private final ConcurrentHashMap<Long, org.jetlang.channels.Channel<Integer>> scannerMap = new ConcurrentHashMap<>();

    public org.jetlang.channels.Channel<Integer> getChannel(long scannerId) {
      return scannerMap.get(scannerId);
    }

    public void addChannel(long scannerId, org.jetlang.channels.Channel<Integer> channel) {
      scannerMap.put(scannerId, channel);
    }
  }

  public class MetaCacheComparable implements Comparator<HRegionInfo> {

    @Override
    public int compare(HRegionInfo o1, HRegionInfo o2) {
      boolean withinStart = false;
      boolean withinEnd = false;


      byte[] o1SR = o1.getStartKey();
      byte[] o1ER = o1.getEndKey();
      if (Arrays.equals(o1SR, o1ER)) {
        byte[] o2SR = o2.getStartKey();
        byte[] o2ER = o2.getEndKey();
        if (o2SR.length == 0 || Bytes.compareTo(o2SR, o1SR) <= 0) {
          withinStart = true;
        }

        if (o2ER.length == 0 || Bytes.compareTo(o2ER, o1SR) > 0) {
          withinEnd = true;
        }


        return withinStart && withinEnd ? 0 : 1;
      } else {
        return o1.compareTo(o2);
      }
    }
  }
}

