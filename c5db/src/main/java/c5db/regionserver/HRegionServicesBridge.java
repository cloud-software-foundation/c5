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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.regionserver.CompactionRequestor;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Leases;
import org.apache.hadoop.hbase.regionserver.RegionServerAccounting;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public class HRegionServicesBridge implements RegionServerServices {

  private final Configuration conf;
  private boolean aborted = false;
  private boolean stopping = false;
  protected static final Logger LOG = LoggerFactory.getLogger(HRegionServicesBridge.class);

  public HRegionServicesBridge(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public boolean isStopping() {
    return stopping;
  }

  @Override
  public HLog getWAL(HRegionInfo regionInfo) throws IOException {
    return null;
  }

  @Override
  public CompactionRequestor getCompactionRequester() {
    return null;
  }

  @Override
  public FlushRequester getFlushRequester() {

    return new FlushRequester() {
      @Override
      public void requestFlush(HRegion region) {
        try {
          region.flushcache();
          LOG.info("FlushCache");
        } catch (IOException e) {
          e.printStackTrace();

        }
      }

      @Override
      public void requestDelayedFlush(HRegion region, long delay) {
        LOG.error("requestDelayedFlush");
      }
    };
  }

  @Override
  public RegionServerAccounting getRegionServerAccounting() {
    RegionServerAccounting regionServerAccounting = new RegionServerAccounting();
    return regionServerAccounting;
  }

  @Override
  public TableLockManager getTableLockManager() {
    return null;
  }

  @Override
  public void postOpenDeployTasks(HRegion r, CatalogTracker ct) throws KeeperException, IOException {
    System.out.println("sdf");

  }

  @Override
  public RpcServerInterface getRpcServer() {
    return null;
  }

  @Override
  public ConcurrentMap<byte[], Boolean> getRegionsInTransitionInRS() {
    return null;
  }

  @Override
  public FileSystem getFileSystem() {
    try {
      FileSystem fileSystem = FileSystem.get(conf);
      return fileSystem;
    } catch (IOException e) {
      e.printStackTrace();

    }
    return null;
  }

  @Override
  public Leases getLeases() {
    return null;
  }

  @Override
  public ExecutorService getExecutorService() {
    return null;
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public ZooKeeperWatcher getZooKeeper() {
    return null;
  }

  @Override
  public CatalogTracker getCatalogTracker() {
    return null;
  }

  @Override
  public ServerName getServerName() {
    return null;
  }

  @Override
  public Map<String, HRegion> getRecoveringRegions() {
    return null;
  }

  @Override
  public void updateRegionFavoredNodesMapping(String encodedRegionName, List<HBaseProtos.ServerName> favoredNodes) {
    System.out.println("sdf");

  }

  @Override
  public InetSocketAddress[] getFavoredNodesForRegion(String encodedRegionName) {
    return new InetSocketAddress[0];
  }

  @Override
  public void addToOnlineRegions(HRegion r) {
    System.out.println("sdf");

  }

  @Override
  public boolean removeFromOnlineRegions(HRegion r, ServerName destination) {
    return false;
  }

  @Override
  public HRegion getFromOnlineRegions(String encodedRegionName) {
    return null;
  }

  @Override
  public List<HRegion> getOnlineRegions(TableName tableName) throws IOException {
    return null;
  }

  @Override
  public void abort(String why, Throwable e) {
    this.aborted = true;
  }

  @Override
  public boolean isAborted() {
    return this.aborted;
  }

  @Override
  public void stop(String why) {
    this.stopping = true;
  }

  @Override
  public boolean isStopped() {
    return stopping;
  }
}
