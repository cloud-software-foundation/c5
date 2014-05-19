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

import c5db.client.generated.Condition;
import c5db.client.generated.MutationProto;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.wal.HLog;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Our interface to a region.
 * <p/>
 * Provides our abstraction to HRegion.
 */
public interface Region {
  /**
   * Creates instances of Region.  This exists to make mocking and testing
   * easier.
   * <p/>
   * Mock out the creator interface - then create/return mock region interfaces.
   */

  boolean mutate(MutationProto mutateProto, Condition condition) throws IOException;

  HRegion getTheRegion();

  RegionScanner getScanner(Scan scan);

  boolean exists(c5db.client.generated.Get get) throws IOException;

  c5db.client.generated.Result get(c5db.client.generated.Get get) throws IOException;

  /**
   * Constructor arguments basically.
   */
  public interface Creator {
    Region getHRegion(
        Path basePath,
        HRegionInfo regionInfo,
        HTableDescriptor tableDescriptor,
        HLog log,
        Configuration conf) throws IOException;
  }
}
