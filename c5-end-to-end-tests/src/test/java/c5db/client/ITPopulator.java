/*
 * Copyright (C) 2013  Ohm Data
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
package c5db.client;

import c5db.C5TestServerConstants;
import c5db.ClusterOrPseudoCluster;
import io.protostuff.ByteString;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class ITPopulator extends ClusterOrPseudoCluster {
  private static ByteString tableName = ByteString.bytesDefaultValue("c5:writeOutAsynchronously");

  @Test
  public void testPopulator() throws IOException {
      int numberOfBatches = 100;
      int batchSize = 10;
      compareToHBasePut(table,
          Bytes.toBytes("cf"),
          Bytes.toBytes("cq"),
          Bytes.toBytes("value"),
          numberOfBatches,
          batchSize);
  }

  private static void compareToHBasePut(final FakeHTable table,
                                        final byte[] cf,
                                        final byte[] cq,
                                        final byte[] value,
                                        final int numberOfBatches,
                                        final int batchSize) throws IOException {

    ArrayList<Put> puts = new ArrayList<>();

    long startTime = System.nanoTime();
    for (int j = 1; j != numberOfBatches + 1; j++) {
      for (int i = 1; i != batchSize + 1; i++) {
        puts.add(new Put(Bytes.vintToBytes(i * j)).add(cf, cq, value));
      }

      int i = 0;
      for (Put put : puts) {
        i++;
        if (i % 1024 == 0) {
          long timeDiff = (System.nanoTime()) - startTime;
          System.out.print("#(" + timeDiff + ")");
          System.out.flush();
          startTime = System.nanoTime();
        }
        if (i % (1024 * 12) == 0) {
          System.out.println("");
        }
        table.put(put);
      }

      puts.clear();
    }
  }

 }
