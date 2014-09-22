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

import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class HBasePop {
  private static final ByteString tableName = ByteString.copyFrom(Bytes.toBytes("tableName"));
  private static final Configuration conf = HBaseConfiguration.create();
  private static final byte[] cf = Bytes.toBytes("cf");

  public static void main(String[] args) throws IOException, InterruptedException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName.toByteArray());
    hTableDescriptor.addFamily(new HColumnDescriptor(cf));
    admin.createTable(hTableDescriptor);

    long start = System.currentTimeMillis();
    compareToHBasePut();
    long end = System.currentTimeMillis();
    System.out.println("time:" + (end - start));
  }

  private static void compareToHBasePut() throws IOException {
    byte[] cq = Bytes.toBytes("cq");
    byte[] value = Bytes.toBytes("value");

    try (HTable table = new HTable(conf, tableName.toByteArray())) {
      ArrayList<Put> puts = new ArrayList<>();
      long startTime = System.nanoTime();
      for (int j = 1; j != 30 + 1; j++) {
        for (int i = 1; i != (1024 * 81) + 1; i++) {
          puts.add(new Put(Bytes.vintToBytes(i * j)).add(cf, cq, value));
        }

        int i = 0;
        for (Put put : puts) {
          i++;
          if (i % 1024 == 0) {
            long timeDiff = (System.nanoTime()) - startTime;

            System.out.print("#(" + timeDiff + ")");
            System.out.flush();
          }
          if (i % (1024 * 20) == 0) {
            System.out.println("");
          }
          table.put(put);
        }
        puts.clear();
      }
    }
  }
}
