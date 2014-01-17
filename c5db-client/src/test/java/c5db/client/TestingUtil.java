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

import c5db.client.OhmTable;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestingUtil {
  ByteString tableName = ByteString.copyFrom(Bytes.toBytes("tableName"));
  OhmTable table;
  HTable hTable;

  byte[] row = Bytes.toBytes("startRow");
  byte[] cf = Bytes.toBytes("cf");
  byte[] cq = Bytes.toBytes("cq");
  byte[] startRow = Bytes.toBytes("startRow");
  byte[] endRow = Bytes.toBytes("endRow");
  byte[] value = Bytes.toBytes("value");

  public TestingUtil() throws IOException, InterruptedException {
    table = new OhmTable(tableName);
  }

  void close() throws IOException {
    table.close();
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    TestingUtil testingUtil = new TestingUtil();

    try {
      testingUtil.testPut();
      testingUtil.testExist();
      testingUtil.testGet();
      testingUtil.testMultiGet();
      testingUtil.testExists();

      testingUtil.testScan();
      testingUtil.close();
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

  public void testGet() throws IOException {
    Result result = table.get(new Get(row).addColumn(cf, cq));
    Result result2 = table.get(new Get(new byte[]{0}).addColumn(cf, cq));
    Result result3 = table.get(new Get(Bytes.add(row, row)).addColumn(cf, cq));

    result.toString();
  }

  public void testPut() throws IOException {
    table.put(new Put(row).add(cf, cq, value));
    table.put(new Put(Bytes.add(row, row)).add(cf, cq, value));
  }

  public void testExist() throws IOException {
    boolean result = table.exists(new Get(row).addColumn(cf, cq));
    boolean result2 = table.exists(new Get(new byte[]{0}).addColumn(cf, cq));
    result = false;
  }

  public void testScan() throws IOException {
    Scan scan = new Scan(row);
    scan.addColumn(cf, cq);
    scan.setStartRow(startRow);
    ResultScanner resultScanner = table.getScanner(scan);
    Result r = resultScanner.next();
    r = resultScanner.next();
    r = resultScanner.next();
  }

  public void testMultiGet() throws IOException {
    List<Get> gets = new ArrayList<>();
    gets.add(new Get(row).addColumn(cf, cq));
    gets.add(new Get(Bytes.add(row, row)).addColumn(cf, cq));
    Result[] result = table.get(gets);

    result.toString();
  }

  public void testExists() throws IOException {
    List<Get> gets = new ArrayList<>();
    gets.add(new Get(row).addColumn(cf, cq));
    gets.add(new Get(Bytes.add(Bytes.add(row, row), row)).addColumn(cf, cq));
    gets.add(new Get(Bytes.add(row, row)).addColumn(cf, cq));
    Boolean[] result = table.exists(gets);

    result.toString();
  }

}
