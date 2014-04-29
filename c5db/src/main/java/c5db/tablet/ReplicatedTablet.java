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

import c5db.interfaces.C5Server;
import c5db.interfaces.ReplicationModule;
import c5db.interfaces.replication.Replicator;
import c5db.interfaces.tablet.TabletStateChange;
import c5db.log.OLogShim;
import c5db.util.C5Futures;
import c5db.util.FiberOnly;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.fibers.Fiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * A tablet, backed by a replicator that keeps values replicated across multiple servers.
 */
public class ReplicatedTablet implements c5db.interfaces.tablet.Tablet {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicatedTablet.class);
  private final C5Server server;

  public void setTabletState(State tabletState) {
    this.tabletState = tabletState;
    publishEvent(tabletState);
  }

  private void setTabletStateFailed(Throwable t) {
    this.tabletState = State.Failed;
    publishEvent(t);
  }


  // Config type info:
  private final HRegionInfo regionInfo;
  private final HTableDescriptor tableDescriptor;
  private final List<Long> peers;
  private final Configuration conf;
  private final Path basePath;

  // Finals
  private final Fiber tabletFiber;
  private final ReplicationModule replicationModule;
  private final Region.Creator regionCreator;

  // State
  private State tabletState;

  private Region region;

  private Replicator replicator;

  public void setStateChangeChannel(Channel<TabletStateChange> stateChangeChannel) {
    this.stateChangeChannel = stateChangeChannel;
  }

  private Channel<TabletStateChange> stateChangeChannel = new MemoryChannel<>();

  public ReplicatedTablet(final C5Server server,
                          final HRegionInfo regionInfo,
                          final HTableDescriptor tableDescriptor,
                          final List<Long> peers,
                          final Path basePath,
                          final Configuration conf,
                          final Fiber tabletFiber,
                          final ReplicationModule replicationModule,
                          final Region.Creator regionCreator) {
    this.server = server;
    this.regionInfo = regionInfo;
    this.tableDescriptor = tableDescriptor;
    this.peers = peers;
    this.conf = conf;
    this.basePath = basePath;

    this.tabletFiber = tabletFiber;
    this.replicationModule = replicationModule;
    this.regionCreator = regionCreator;

    this.tabletState = State.Initialized;
  }

  @Override
  public void start() {
    this.tabletFiber.start();
    this.tabletFiber.execute(this::createReplicator);
  }

  @FiberOnly
  private void createReplicator() {
    assert tabletState == State.Initialized;

    ListenableFuture<Replicator> future =
        replicationModule.createReplicator(regionInfo.getRegionNameAsString(), peers);

    C5Futures.addCallback(future, this::replicatorCreated, this::handleFail, tabletFiber);

    setTabletState(State.CreatingReplicator);
  }

  private void replicatorCreated(Replicator replicator) {
    assert tabletState == State.CreatingReplicator;

    this.replicator = replicator;
    Channel<Replicator.State> replicatorStateChannel = replicator.getStateChannel();
    replicatorStateChannel.subscribe(tabletFiber, this::tabletStateChangeCallback);

    this.replicator.start();

    OLogShim shim = new OLogShim(replicator);

    try {
      region = regionCreator.getHRegion(basePath, regionInfo, tableDescriptor, shim, conf);
      setTabletState(State.Open);
    } catch (IOException e) {
      handleFail(e);
    }
  }

  private void tabletStateChangeCallback(Replicator.State state) {
    if (state.equals(Replicator.State.LEADER)) {
      if (this.getRegionInfo().getRegionNameAsString().startsWith("hbase:root,")) {
        RootTabletLeaderBehavior rootTabletLeaderBehavior = new RootTabletLeaderBehavior(this, server);
        try {
          rootTabletLeaderBehavior.start();
        } catch (IOException e) {
          e.printStackTrace();
          System.exit(0);
        }
        this.setTabletState(State.Leader);
      } else {
        this.setTabletState(State.Leader);
      }
    }
  }

  private void publishEvent(State newState) {
    getStateChangeChannel().publish(new TabletStateChange(this, newState, null));
  }

  private void publishEvent(Throwable t) {
    getStateChangeChannel().publish(new TabletStateChange(this, State.Failed, t));
  }

  private void handleFail(Throwable t) {

    tabletFiber.dispose();
    setTabletStateFailed(t);
  }

  @Override
  public Channel<TabletStateChange> getStateChangeChannel() {
    return this.stateChangeChannel;
  }

  @Override
  public boolean isOpen() {
    return tabletState == State.Open;
  }

  @Override
  public State getTabletState() {
    return tabletState;
  }

  public void dispose() {
    this.tabletFiber.dispose();
  }

  @Override
  public HRegionInfo getRegionInfo() {
    return this.regionInfo;
  }

  @Override
  public HTableDescriptor getTableDescriptor() {
    return tableDescriptor;
  }

  @Override
  public List<Long> getPeers() {
    return peers;
  }

  @Override
  public Region getRegion() {
    return region;
  }
}
