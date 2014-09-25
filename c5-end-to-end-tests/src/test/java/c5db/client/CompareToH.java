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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class CompareToH extends ClusterOrPseudoCluster {
  private static HTable hTable;
  private static ByteString tableName =
      ByteString.copyFrom(Bytes.toBytes("tableName"));
  private static Configuration conf;

  private final byte[] cf = Bytes.toBytes("cf");

  private CompareToH() {
    conf = HBaseConfiguration.create();
  }

  public static void main(String[] args) throws IOException, InterruptedException, TimeoutException, ExecutionException {
    CompareToH testingUtil = new CompareToH();
    hTable = new HTable(conf, tableName.toByteArray());
    testingUtil.compareToHBaseScan();
    hTable.close();
  }

  void compareToHBaseScan() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    tableName = ByteString.copyFrom(Bytes.toBytes(name.getMethodName()));

    FakeHTable table = new FakeHTable(C5TestServerConstants.LOCALHOST, getRegionServerPort(), tableName);

    long As, Ae, Bs, Be;
    int i = 0;
    Result result;
    ResultScanner scanner;


    scanner = table.getScanner(cf);
    As = System.currentTimeMillis();
    do {
      i++;
      if (i % 1024 == 0) {
        System.out.print("#");
        System.out.flush();
      }
      if (i % (1024 * 80) == 0) {
        System.out.println("");
      }
      result = scanner.next();
    } while (result != null);
    scanner.close();
    Ae = System.currentTimeMillis();

    int j = 0;
    Bs = System.currentTimeMillis();
    scanner = hTable.getScanner(cf);
    do {
      j++;
      if (j % 1024 == 0) {
        System.out.print("!");
        System.out.flush();
      }
      if (j % (1024 * 80) == 0) {
        System.out.println("");
      }
      result = scanner.next();
    } while (result != null);
    Be = System.currentTimeMillis();

    scanner.close();

    System.out.println("A:" + String.valueOf(Ae - As) + "i:" + i);
    System.out.println("B:" + String.valueOf(Be - Bs) + "j:" + j);
    table.close();
  }
}