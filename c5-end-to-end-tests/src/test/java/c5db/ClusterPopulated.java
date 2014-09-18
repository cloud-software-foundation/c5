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
package c5db;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;

import java.io.IOException;

public class ClusterPopulated extends ClusterOrPseudoCluster {

  public final int NUMBER_OF_ROWS = 101;

  @Before
  public void initTable() throws IOException {
    for (int i = 0; i != NUMBER_OF_ROWS; i++) {
      Put put = new Put(Bytes.toBytes(i));
      put.add(Bytes.toBytes("cf"), Bytes.toBytes("cq"), new byte[2]);
      this.table.put(put);
    }
  }
}