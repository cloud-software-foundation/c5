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
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * language to help verify data from the database.
 * <p/>
 * Add additional static verbs as necessary!
 */
class DataHelper {
  private static final byte[] cf = Bytes.toBytes("cf");
  private static final byte[] cq = Bytes.toBytes("cq");

  static byte[][] valuesReadFromDB(FakeHTable hTable, byte[][] row) throws IOException {
    List<Get> gets = Arrays.asList(row).stream().map(getRow -> {
      Get get = new Get(getRow);
      get.addColumn(cf, cq);
      return get;
    }).collect(toList());

    Result[] results = hTable.get(gets);

    List<byte[]> values = Arrays
        .asList(results)
        .stream()
        .map(result -> result.getValue(cf, cq)).collect(toList());
    return values.toArray(new byte[values.size()][]);

  }

  static Boolean[] valuesExistsInDB(FakeHTable hTable, byte[][] row) throws IOException {
    List<Get> gets = Arrays.asList(row).stream().map(getRow -> {
      Get get = new Get(getRow);
      get.addColumn(cf, cq);
      return get;
    }).collect(toList());

    return hTable.exists(gets);
  }

  static byte[] valueReadFromDB(FakeHTable hTable, byte[] row) throws IOException {
    Get get = new Get(row);
    get.addColumn(cf, cq);
    Result result = hTable.get(get);
    return result.getValue(cf, cq);
  }

  static boolean valueExistsInDB(FakeHTable hTable, byte[] row) throws IOException {
    Get get = new Get(row);
    get.addColumn(cf, cq);
    return hTable.exists(get);
  }

  static void putRowInDB(FakeHTable hTable, byte[] row) throws IOException {
    Put put = new Put(row);
    put.add(cf, cq, ClusterOrPseudoCluster.value);
    hTable.put(put);
  }

  static void deleteRowFamilyInDB(FakeHTable hTable, byte[] row) throws IOException {
    Delete delete = new Delete(row);
    delete.deleteFamily(cf);
    hTable.delete(delete);
  }

  static void putBigRowInDatabase(FakeHTable hTable, byte[] row) throws IOException {
    Put put = new Put(row).add(cf, cq, new byte[1024 * 1024 * 64]);
    hTable.put(put);
  }

  static void putRowAndValueIntoDatabase(FakeHTable hTable,
                                         byte[] row,
                                         byte[] valuePutIntoDatabase) throws IOException {
    Put put = new Put(row);
    put.add(cf, cq, valuePutIntoDatabase);
    hTable.put(put);
  }

  static void putsRowInDB(FakeHTable hTable, byte[][] rows, byte[] value) throws IOException {
    Stream<Put> puts = Arrays.asList(rows).stream().map(row -> new Put(row).add(cf, cq, value));
    hTable.put(puts.collect(toList()));
  }

  static boolean checkAndPutRowAndValueIntoDatabase(FakeHTable hTable,
                                                    byte[] row,
                                                    byte[] valueToCheck,
                                                    byte[] valuePutIntoDatabase) throws IOException {
    Put put = new Put(row);
    put.add(cf, cq, valuePutIntoDatabase);
    return hTable.checkAndPut(row, cf, cq, valueToCheck, put);
  }

  static boolean checkAndDeleteRowAndValueIntoDatabase(FakeHTable hTable,
                                                       byte[] row,
                                                       byte[] valueToCheck) throws IOException {
    Delete delete = new Delete(row);
    return hTable.checkAndDelete(row, cf, cq, valueToCheck, delete);
  }

  static ResultScanner getScanner(FakeHTable hTable, byte[] row) throws IOException {
    Scan scan = new Scan(row);
    scan.addColumn(cf, cq);
    return hTable.getScanner(scan);
  }

  static byte[] nextResult(ResultScanner resultScanner) throws IOException {
    return resultScanner.next().getValue(cf, cq);
  }
}
