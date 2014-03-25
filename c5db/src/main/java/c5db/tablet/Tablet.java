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

import c5db.interfaces.ReplicationModule;
import c5db.util.C5Futures;
import c5db.util.FiberOnly;
import com.google.common.util.concurrent.ListenableFuture;
import org.jetlang.fibers.Fiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * A tablet, responsible for lifecycle of a tablet, creation of said tablet, etc.
 */
public class Tablet {
  private static final Logger LOG = LoggerFactory.getLogger(Tablet.class);

  public enum State {
    Initialized, // Initial state, nothing done yet.
    CreatingReplicator, // Waiting for replication instance to be created
    Open,  // Ready to service requests.
  }

  // Finals
  private final Fiber tabletFiber;
  private final ReplicationModule replicationModule;
  private final IRegion.Creator regionCreator;

  // State
  private State tabletState;


  private IRegion region;

  private ReplicationModule.Replicator replicator;

  public Tablet(Fiber tabletFiber, ReplicationModule replicationModule,
                IRegion.Creator regionCreator) {
    this.tabletFiber = tabletFiber;
    this.replicationModule = replicationModule;
    this.regionCreator = regionCreator;
    tabletState = State.Initialized;

    this.tabletFiber.start();
    this.tabletFiber.execute(this::createReplicator);
  }

  @FiberOnly
  private void createReplicator() {
    assert tabletState == State.Initialized;

    // TODO look this data up!

    ListenableFuture<ReplicationModule.Replicator> future =
        replicationModule.createReplicator("", new ArrayList<>());

    C5Futures.addCallback(future, this::replicatorCreated, this::handleFail, tabletFiber);

    tabletState = State.CreatingReplicator;
  }

  private void replicatorCreated(ReplicationModule.Replicator replicator) {
    assert tabletState == State.CreatingReplicator;

    tabletState = State.Open;
  }

  private void handleFail(Throwable t) {

    // TODO tell someone else!

    tabletFiber.dispose();
  }

  public boolean isOpen() {
    return tabletState == State.Open;
  }

  public State getTabletState() {
    return tabletState;
  }

  public void dispose() {
    this.tabletFiber.dispose();
  }
}
