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
 *
 *  This file incorporates work covered by the following copyright and
 *  permission notice:
 */
package ohmdb.client;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class TestInOrderScan {
  private static ByteString tableName =
      ByteString.copyFrom(Bytes.toBytes("tableName"));

  byte[] cf = Bytes.toBytes("cf");

  public TestInOrderScan() throws IOException, InterruptedException {
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    TestInOrderScan testingUtil = new TestInOrderScan();
    testingUtil.compareToHBaseScan();
  }

  public void compareToHBaseScan() throws IOException, InterruptedException {
    OhmTable table = new OhmTable(tableName);

    Result result = null;
    ResultScanner scanner;

    scanner = table.getScanner(cf);
    byte[] previousRow = {};
    int counter = 0;
    do {
      if (result != null) {
        previousRow = result.getRow();
      }
      result = scanner.next();

      if (Bytes.compareTo(result.getRow(), previousRow) < 1) {
        System.out.println(counter);
        System.exit(1);
      }

    } while (result != null);
    table.close();
  }
}
