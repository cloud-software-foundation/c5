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
package c5db.interfaces;

import c5db.messages.generated.ModuleType;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.jetlang.channels.Channel;

import java.util.List;

/**
 * Manages the lifecycle of tablets - the individual chunks of tables.  Each tablet is
 * an ordered portion of the key space, and has a distinct lifecycle.  This module handles
 * coordination with other modules.
 * <p>
 * Roughly, to bring a tablet online, first a replicator instance must be created for it. Then it
 * must be bound to the write-ahead-log.  Finally the local tablet files must be located,
 * verified and loaded.
 */
@DependsOn(ReplicationModule.class)
@ModuleTypeBinding(ModuleType.Tablet)
public interface TabletModule extends C5Module {

  public HRegion getTablet(String tabletName);

  // TODO this interface is not strong enough. Need HRegionInfo etc.
  public void startTablet(List<Long> peers, String tabletName);

  public Channel<TabletStateChange> getTabletStateChanges();

  public static class TabletStateChange {
    public final Tablet tablet;
    public final Tablet.State state;
    public final Throwable optError;

    public TabletStateChange(Tablet tablet, Tablet.State state, Throwable optError) {
      this.tablet = tablet;
      this.state = state;
      this.optError = optError;
    }

    @Override
    public String toString() {
      return "TabletStateChange{" +
          "tablet=" + tablet +
          ", state=" + state +
          ", optError=" + optError +
          '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TabletStateChange that = (TabletStateChange) o;

      if (optError != null ? !optError.equals(that.optError) : that.optError != null) return false;
      if (state != that.state) return false;
      if (!tablet.equals(that.tablet)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = tablet.hashCode();
      result = 31 * result + state.hashCode();
      result = 31 * result + (optError != null ? optError.hashCode() : 0);
      return result;
    }
  }

  interface Tablet {
    Channel<TabletStateChange> getStateChangeChannel();

    boolean isOpen();

    c5db.tablet.Tablet.State getTabletState();

    HRegionInfo getRegionInfo();

    HTableDescriptor getTableDescriptor();

    List<Long> getPeers();

    enum State {
      Initialized, // Initial state, nothing done yet.
      CreatingReplicator, // Waiting for replication instance to be created
      Open,   // Ready to service requests.
      Failed,
    }
  }
}
