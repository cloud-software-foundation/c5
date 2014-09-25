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

package c5db;

import c5db.client.FakeHTable;
import io.protostuff.ByteString;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class ITClusters extends ClusterPopulated {

  @Ignore
  @Test(timeout = 1000)
  public void metaTableShouldContainUserTableEntries()
      throws InterruptedException, ExecutionException, TimeoutException, IOException {
    Scan scan = new Scan();
    scan.addFamily(HConstants.CATALOG_FAMILY);
    FakeHTable metaTable = new FakeHTable("localhost", metaOnPort, ByteString.copyFromUtf8("hbase:meta"));
    ResultScanner scanner = metaTable.getScanner(scan);

    Result result;
    int counter = 0;
    do {
      result = scanner.next();
      counter++;
      if (result == null) {
        break;
      }
      assertThat(result, ScanMatchers.isWellFormedUserTable(name));
    } while (true);
    assertThat(counter, is(this.splitkeys.length + 2));
    scanner.close();
  }
}