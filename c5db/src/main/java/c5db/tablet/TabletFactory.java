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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;

import java.nio.file.Path;
import java.util.List;

/**
 * Essentially the constructor parameters for a tablet, as an interface. This exists primarily
 * to provide testability for those who manage the lifecycle of Tablets.
 * <p>
 * In production code, one can just type Tablet::new !!
 */
public interface TabletFactory {
  c5db.interfaces.tablet.Tablet create(
      C5Server server,
      HRegionInfo regionInfo,
      HTableDescriptor tableDescriptor,
      List<Long> peers,
      Path basePath,
      Configuration legacyConf,
      ReplicationModule replicationModule,
      Region.Creator regionCreator);
}
