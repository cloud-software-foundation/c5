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