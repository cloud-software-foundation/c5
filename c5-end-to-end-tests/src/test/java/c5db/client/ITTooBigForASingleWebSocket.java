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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static c5db.testing.BytesMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ITTooBigForASingleWebSocket extends ClusterOrPseudoCluster {
  private static final byte[] randomBytes = new byte[65535 * 4];
  private static final Random random = new Random();

  static {
    random.nextBytes(randomBytes);
  }

  private final byte[] cf = Bytes.toBytes("cf");
  private final byte[] cq = Bytes.toBytes("cq");

  @Test
  public void shouldSuccessfullyAcceptSmallPut() throws InterruptedException, ExecutionException, TimeoutException, IOException, MutationFailedException {
    DataHelper.putRowInDB(table, row);
  }

  @Test
  public void shouldSuccessfullyAcceptSmallPutAndReadSameValue()
      throws InterruptedException, ExecutionException, TimeoutException, MutationFailedException, IOException {
    byte[] valuePutIntoDatabase = row;
    putRowAndValueIntoDatabase(row, valuePutIntoDatabase);
    assertThat(DataHelper.valueReadFromDB(table, row), is(equalTo(valuePutIntoDatabase)));
  }

  @Test
  public void testSendBigOne()
      throws InterruptedException, ExecutionException, TimeoutException, IOException, MutationFailedException {
    byte[] valuePutIntoDatabase = randomBytes;
    putRowAndValueIntoDatabase(row, valuePutIntoDatabase);
  }

  @Test
  public void shouldSuccessfullyAcceptLargePutAndReadSameValue() throws IOException {
    byte[] valuePutIntoDatabase = randomBytes;
    putRowAndValueIntoDatabase(row, valuePutIntoDatabase);
    assertThat(DataHelper.valueReadFromDB(table, row), is(equalTo(valuePutIntoDatabase)));
  }

  private void putRowAndValueIntoDatabase(byte[] row, byte[] valuePutIntoDatabase) throws IOException {
    Put put = new Put(row);
    put.add(cf, cq, valuePutIntoDatabase);
    table.put(put);
  }
}