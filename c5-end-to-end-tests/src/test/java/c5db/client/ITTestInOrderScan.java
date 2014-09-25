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

import c5db.ClusterOrPseudoCluster;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static c5db.client.DataHelper.putRowInDB;
import static junit.framework.TestCase.assertFalse;

@Ignore
public class ITTestInOrderScan extends ClusterOrPseudoCluster {

  private final byte[] cf = Bytes.toBytes("cf");

  @Test
  public void testInOrderScan() throws IOException, InterruptedException, TimeoutException, ExecutionException {
    Result result = null;
    ResultScanner scanner;
    putRowInDB(table, row);
    row = Bytes.add(row, row);
    putRowInDB(table, row);
    row = Bytes.add(row, row);
    putRowInDB(table, row);
    row = Bytes.add(row, row);
    putRowInDB(table, row);
    row = Bytes.add(row, row);
    putRowInDB(table, row);
    row = Bytes.add(row, row);
    putRowInDB(table, row);

    scanner = table.getScanner(cf);
    byte[] previousRow = {};
    do {
      if (result != null) {
        previousRow = result.getRow();
      }
      result = scanner.next();
      if (result != null) {
        assertFalse(Bytes.compareTo(result.getRow(), previousRow) < 1);
      }
    } while (result != null);
  }
}
