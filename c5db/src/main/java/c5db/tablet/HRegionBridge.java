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

import c5db.regionserver.ReverseProtobufUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.IOException;

/**
 * Bridge between the (complex) HRegion and the rest of c5.
 * <p/>
 * Provides an abstraction and test point, and lessons in how to abstract
 * and extract HRegion functionality.
 */
public class HRegionBridge implements Region {

  private HRegion theRegion;

  public HRegionBridge(final HRegion theRegion) {
    this.theRegion = theRegion;
  }

  @Override
  public void put(final Put put) throws IOException {
    theRegion.put(put);
  }

  @Override
  public HRegion getTheRegion() {
    return theRegion;
  }

  @Override
  public RegionScanner getScanner(Scan scan) {
    try {
      return getTheRegion().getScanner(scan);
    } catch (IOException e) {
      e.printStackTrace();

    }
    return null;
  }

  @Override
  public boolean exists(c5db.client.generated.Get get) throws IOException {
    final org.apache.hadoop.hbase.client.Get serverGet = ReverseProtobufUtil.toGet(get);
    Result result = this.getTheRegion().get(serverGet);
    return result.getExists();
  }

  @Override
  public c5db.client.generated.Result get(c5db.client.generated.Get get) throws IOException {
    final org.apache.hadoop.hbase.client.Get serverGet = ReverseProtobufUtil.toGet(get);
    return ReverseProtobufUtil.toResult(this.getTheRegion().get(serverGet));
  }
}
