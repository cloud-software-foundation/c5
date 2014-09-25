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

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;

import java.io.IOException;

public class ClusterPopulated extends ClusterOrPseudoCluster {

  public final int NUMBER_OF_ROWS = 101;

  @Before
  public void initTable() throws IOException {
    for (int i = 0; i != NUMBER_OF_ROWS; i++) {
      Put put = new Put(Bytes.toBytes(i));
      put.add(Bytes.toBytes("cf"), Bytes.toBytes("cq"), new byte[2]);
      this.table.put(put);
    }
  }
}