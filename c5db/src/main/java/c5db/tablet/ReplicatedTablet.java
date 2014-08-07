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
import c5db.interfaces.C5Server;
import c5db.interfaces.ReplicationModule;
import c5db.interfaces.replication.Replicator;
import c5db.interfaces.replication.ReplicatorInstanceEvent;
import c5db.interfaces.tablet.TabletStateChange;
import c5db.log.OLogShim;
import c5db.replication.C5GeneralizedReplicator;
import c5db.tablet.tabletCreationBehaviors.MetaTabletLeaderBehavior;
import c5db.tablet.tabletCreationBehaviors.RootTabletLeaderBehavior;
import c5db.util.C5Futures;
import c5db.util.FiberOnly;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.Subscriber;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
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
  private long leader;

  private Channel<TabletStateChange> stateChangeChannel = new MemoryChannel<>();

  void setTabletState(State tabletState) {
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
  private final Fiber shimFiber = new ThreadFiber();
  private final ReplicationModule replicationModule;
  private final Region.Creator regionCreator;

  // State
  private State tabletState;

  private Region region;

  public void setStateChangeChannel(Channel<TabletStateChange> stateChangeChannel) {
    this.stateChangeChannel = stateChangeChannel;
  }

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
    shimFiber.start();
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

    Subscriber<Replicator.State> replicatorStateChannel = replicator.getStateChannel();
    replicatorStateChannel.subscribe(tabletFiber, this::tabletStateCallback);
    Subscriber<ReplicatorInstanceEvent> replicatorEventChannel = replicator.getEventChannel();
    replicatorEventChannel.subscribe(tabletFiber, this::tabletStateChangeCallback);
    replicator.start();

    // TODO this ThreadFiber is a workaround until issue 252 is fixed; at which point shim can use tabletFiber.
    OLogShim shim = new OLogShim(new C5GeneralizedReplicator(replicator, shimFiber));
    try {
      region = regionCreator.getHRegion(basePath, regionInfo, tableDescriptor, shim, conf);
      setTabletState(State.Open);
    } catch (IOException e) {
      setTabletState(State.Failed);
      LOG.error("Settings tablet state to failed, we got an IOError opening the region:" + e.toString());
    }


  }

  private void tabletStateChangeCallback(ReplicatorInstanceEvent replicatorInstanceEvent) {
    switch (replicatorInstanceEvent.eventType) {
      case QUORUM_START:
        break;
      case LEADER_ELECTED:
        this.leader = replicatorInstanceEvent.newLeader;
        break;
      case ELECTION_TIMEOUT:
        break;
      case QUORUM_FAILURE:
        break;
      case LEADER_DEPOSED:
        this.leader = replicatorInstanceEvent.newLeader;
        break;
    }
  }

  private void tabletStateCallback(Replicator.State state) {
    switch (state) {
      case INIT:
        break;
      case FOLLOWER:
        this.setTabletState(State.Open);
        break;
      case CANDIDATE:
        this.setTabletState(State.Open);
        break;
      case LEADER:
        this.setTabletState(State.Leader);
        try {
          if (this.getRegionInfo().getRegionNameAsString().startsWith("hbase:root,")) {

            long numberOfMetaPeers = server.isSingleNodeMode() ? 1 : C5ServerConstants.DEFAULT_QUORUM_SIZE;
            RootTabletLeaderBehavior rootTabletLeaderBehavior = new RootTabletLeaderBehavior(this,
                server,
                numberOfMetaPeers);
            rootTabletLeaderBehavior.start();

          } else if (this.getRegionInfo().getRegionNameAsString().startsWith("hbase:meta,")) {
            // Have the meta leader update the root region with it being marked as the leader
            MetaTabletLeaderBehavior metaTabletLeaderBehavior = new MetaTabletLeaderBehavior(this, server);
            metaTabletLeaderBehavior.start();

          } else {
            // update the meta table with my leader status
          }
        } catch (Exception e) {
          LOG.error("Error setting tablet state to leader", e);
        }
        break;
    }

  }

  private void publishEvent(State newState) {
    stateChangeChannel.publish(new TabletStateChange(this, newState, null));
  }

  private void publishEvent(Throwable t) {
    stateChangeChannel.publish(new TabletStateChange(this, State.Failed, t));
  }

  private void handleFail(Throwable t) {
    tabletFiber.dispose();
    setTabletStateFailed(t);
  }

  @Override
  public Subscriber<TabletStateChange> getStateChangeChannel() {
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
    shimFiber.dispose();
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
  public long getLeader() {
    return leader;
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
