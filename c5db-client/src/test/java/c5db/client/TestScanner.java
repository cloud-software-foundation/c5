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
package c5db.client;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class TestScanner {
  ByteString tableName = ByteString.copyFrom(Bytes.toBytes("tableName"));
  C5Table table;

  public TestScanner() throws IOException, InterruptedException {
    table = new C5Table(tableName);
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    TestScanner testingUtil = new TestScanner();
    testingUtil.scan();
  }

  public void scan() throws IOException {
    int i = 0;
    Result result;

    ResultScanner scanner = table.getScanner(new Scan().setStartRow(new byte[]{0x00}));
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
    table.close();
  }
}