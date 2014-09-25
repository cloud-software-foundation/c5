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
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static c5db.client.DataHelper.putRowInDB;
import static c5db.client.DataHelper.valueExistsInDB;
import static c5db.client.DataHelper.valueReadFromDB;
import static c5db.client.DataHelper.valuesExistsInDB;
import static c5db.client.DataHelper.valuesReadFromDB;
import static c5db.testing.BytesMatchers.equalTo;
import static junit.framework.Assert.assertNull;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;

public class ITTestingUtil extends ClusterOrPseudoCluster {

  @Test
  public void testSimplePutGet() throws IOException {
    putRowInDB(table, row);
    assertThat(valueReadFromDB(table, row), is(equalTo(value)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidPut() throws IOException, InterruptedException, ExecutionException, TimeoutException, MutationFailedException {
    putRowInDB(table, new byte[]{});
  }

  // TODO Should be an IllegalArgument HBase!!!
  @Test(expected = NullPointerException.class)
  public void testNullPut() throws IOException, InterruptedException, ExecutionException, TimeoutException, MutationFailedException {
    putRowInDB(table, null);
  }

  @Test
  public void testExist() throws IOException {
    final byte[] randomBytesNeverInsertedInDB = {0x00, 0x01, 0x02};
    putRowInDB(table, row);
    assertTrue(valueExistsInDB(table, row));
    assertFalse(valueExistsInDB(table, randomBytesNeverInsertedInDB));
  }


  @Test(expected = IllegalArgumentException.class)
  public void testInvalidExist() throws IOException {
    putRowInDB(table, row);
    assertTrue(valueExistsInDB(table, row));
    valueExistsInDB(table, new byte[]{});
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullExist() throws IOException {
    putRowInDB(table, row);
    assertTrue(valueExistsInDB(table, row));
    valueExistsInDB(table, null);
  }

  @Test
  public void testMultiGet() throws IOException {
    byte[] neverInserted = Bytes.toBytes("rowNeverInserted");
    putRowInDB(table, row);
    byte[][] values = valuesReadFromDB(table, new byte[][]{row, neverInserted});
    assertArrayEquals(value, values[0]);
    assertNull(values[1]);
  }

  @Test
  public void testMultiExists() throws IOException, InterruptedException, TimeoutException, ExecutionException, MutationFailedException {
    byte[] neverInserted = Bytes.toBytes("rowNeverInserted");
    putRowInDB(table, row);
    Boolean[] values = valuesExistsInDB(table, new byte[][]{row, neverInserted});
    assertTrue(values[0]);
    assertFalse(values[1]);
  }


  @Ignore
  @Test(timeout = 1000)
  public void testScan() throws IOException {
    byte[] row0 = new byte[]{0x00};
    byte[] row1 = new byte[]{0x01};
    byte[] row2 = new byte[]{0x02};
    byte[] row3 = new byte[]{0x03};
    byte[] row10 = new byte[]{0x0a};
    byte[] row11 = new byte[]{0x0b};
    byte[] row12 = new byte[]{0x0c};

    putRowInDB(table, row0);
    putRowInDB(table, row1);
    putRowInDB(table, row2);
    putRowInDB(table, row3);
    putRowInDB(table, row10);
    putRowInDB(table, row11);
    putRowInDB(table, row12);

    Scan scan = new Scan(row1);
    scan.setStopRow(row3);
    ResultScanner resultScanner = table.getScanner(scan);

    assertArrayEquals(resultScanner.next().getRow(), row1);
    assertArrayEquals(resultScanner.next().getRow(), row2);
    assertEquals(resultScanner.next(), null);
    assertEquals(resultScanner.next(), null);
  }

  @Ignore
  @Test(timeout = 1000)
  public void testScanWith0Row() throws IOException {
    byte[] row0 = new byte[]{0x00};
    byte[] row1 = new byte[]{0x01};
    byte[] row2 = new byte[]{0x02};
    byte[] row3 = new byte[]{0x03};
    byte[] row10 = new byte[]{0x0a};
    byte[] row11 = new byte[]{0x0b};
    byte[] row12 = new byte[]{0x0c};

    putRowInDB(table, row0);
    putRowInDB(table, row1);
    putRowInDB(table, row2);
    putRowInDB(table, row3);
    putRowInDB(table, row10);
    putRowInDB(table, row11);
    putRowInDB(table, row12);

    Scan scan = new Scan();
    scan.setStartRow(new byte[]{0x00});
    scan.setStopRow(row3);
    ResultScanner resultScanner = table.getScanner(scan);

    assertArrayEquals(row0, resultScanner.next().getRow());
    assertArrayEquals(row1, resultScanner.next().getRow());
    assertArrayEquals(row2, resultScanner.next().getRow());
    assertEquals(resultScanner.next(), null);
  }

  @Ignore
  @Test
  public void testScanWithNoStart() throws IOException {
    byte[] row0 = new byte[]{0x00};
    byte[] row1 = new byte[]{0x01};
    byte[] row2 = new byte[]{0x02};
    byte[] row3 = new byte[]{0x03};
    byte[] row10 = new byte[]{0x0a};
    byte[] row11 = new byte[]{0x0b};
    byte[] row12 = new byte[]{0x0c};

    putRowInDB(table, row0);
    putRowInDB(table, row1);
    putRowInDB(table, row2);
    putRowInDB(table, row3);
    putRowInDB(table, row10);
    putRowInDB(table, row11);
    putRowInDB(table, row12);

    Scan scan = new Scan();
    scan.setStopRow(row3);
    ResultScanner resultScanner = table.getScanner(scan);

    assertArrayEquals(row0, resultScanner.next().getRow());
    assertArrayEquals(row1, resultScanner.next().getRow());
    assertArrayEquals(row2, resultScanner.next().getRow());
    assertEquals(resultScanner.next(), null);
  }
}