/*
 * Copyright 2014 WANdisco
 *
 *  WANdisco licenses this file to you under the Apache License,
 *  version 2.0 (the "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations
 *  under the License.
 */
package c5db.tablet.hregionbridge;

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
    return new RegionServerAccounting();
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
