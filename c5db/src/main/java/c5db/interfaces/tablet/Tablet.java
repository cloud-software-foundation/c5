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
package c5db.interfaces.tablet;


import c5db.client.generated.RegionSpecifier;
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

  RegionSpecifier getRegionSpecifier();

  boolean rowInRange(byte[] row);

  enum State {
    Initialized, // Initial state, nothing done yet.
    CreatingReplicator, // Waiting for replication instance to be created
    Open,   // Ready to service requests.
    Failed,
    Leader,
  }
}
