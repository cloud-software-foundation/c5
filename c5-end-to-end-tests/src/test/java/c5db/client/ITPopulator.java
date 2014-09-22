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
