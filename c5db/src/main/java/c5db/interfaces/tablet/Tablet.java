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
package c5db.interfaces.tablet;

import c5db.tablet.Region;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.jetlang.channels.Channel;
import org.jetlang.channels.Subscriber;

import java.util.List;

/**
 * A tablet object
 */
public interface Tablet {
  void start();

  Subscriber<TabletStateChange> getStateChangeChannel();

  boolean isOpen();

  State getTabletState();

  HRegionInfo getRegionInfo();

  HTableDescriptor getTableDescriptor();

  long getLeader();

  List<Long> getPeers();

  Region getRegion();

  void setStateChangeChannel(Channel<TabletStateChange> stateChangeChannel);

  enum State {
    Initialized, // Initial state, nothing done yet.
    CreatingReplicator, // Waiting for replication instance to be created
    Open,   // Ready to service requests.
    Failed,
    Leader,
  }
}
