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

import c5db.client.generated.RegionSpecifier;
import c5db.interfaces.tablet.Tablet;
import c5db.interfaces.tablet.TabletStateChange;
import c5db.messages.generated.ModuleType;
import c5db.regionserver.RegionNotFoundException;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.jetbrains.annotations.NotNull;
import org.jetlang.channels.Channel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

/**
 * Manages the lifecycle of tablets - the individual chunks of tables.  Each tablet is
 * an ordered portion of the key space, and has a distinct lifecycle.  This module handles
 * coordination with other modules.
 * <p/>
 * Roughly, to bring a tablet online, first a replicator instance must be created for it. Then it
 * must be bound to the write-ahead-log.  Finally the local tablet files must be located,
 * verified and loaded.
 */
@DependsOn(ReplicationModule.class)
@ModuleTypeBinding(ModuleType.Tablet)
public interface TabletModule extends C5Module {
  public Channel<TabletStateChange> getTabletStateChanges();

  @NotNull
  Tablet getTablet(String tableName, ByteBuffer row) throws RegionNotFoundException;

  Tablet getTablet(RegionSpecifier regionSpecifier) throws RegionNotFoundException;

  Collection<Tablet> getTablets();

  void startTabletHere(HTableDescriptor hTableDescriptor,
                       HRegionInfo hRegionInfo,
                       ImmutableList<Long> peers) throws IOException;
}