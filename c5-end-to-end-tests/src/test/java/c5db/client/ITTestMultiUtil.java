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
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.core.IsNull;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static c5db.testing.BytesMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ITTestMultiUtil extends ClusterOrPseudoCluster {
  private final byte[] row1 = Bytes.toBytes("row1");
  private final byte[] row2 = Bytes.toBytes("row2");

  @Test
  public void testMultiPut() throws IOException {
    DataHelper.putsRowInDB(table, new byte[][]{row1, row2}, value);
    assertThat(DataHelper.valueReadFromDB(table, row), is(not(equalTo(value))));
    assertThat(DataHelper.valueReadFromDB(table, row1), is(equalTo(value)));
    assertThat(DataHelper.valueReadFromDB(table, row2), is(equalTo(value)));
  }

  @Ignore
  @Test
  public void testScan() throws IOException {
    DataHelper.putsRowInDB(table, new byte[][]{row1, row2}, value);
    ResultScanner resultScanner = DataHelper.getScanner(table, new byte[]{});
    assertThat(DataHelper.nextResult(resultScanner), is(equalTo(value)));
    assertThat(DataHelper.nextResult(resultScanner), is(equalTo(value)));
  }

  @Test
  public void testMultiDelete() throws IOException {
    DataHelper.putsRowInDB(table, new byte[][]{row1, row2}, value);
    assertThat(DataHelper.valueReadFromDB(table, row1), is(equalTo(value)));
    assertThat(DataHelper.valueReadFromDB(table, row2), is(equalTo(value)));
    DataHelper.deleteRowFamilyInDB(table, row1);
    DataHelper.deleteRowFamilyInDB(table, row2);
    assertThat(DataHelper.valueReadFromDB(table, row1), is(not(equalTo(value))));
    assertThat(DataHelper.valueReadFromDB(table, row2), is(not(equalTo(value))));
  }


  @Test
  public void testMultiRowAtomicMagic() throws IOException {
    RowMutations rowMutations = new RowMutations(row1);
    byte[] cf = Bytes.toBytes("cf");
    byte[] cq = Bytes.toBytes("cq");

    rowMutations.add(new Put(row1).add(cf, cq, value));
    rowMutations.add(new Put(row1).add(cf, Bytes.add(cq, cq), value));

    table.mutateRow(rowMutations);

    assertThat(DataHelper.valueReadFromDB(table, row1), is(equalTo(value)));
    DataHelper.deleteRowFamilyInDB(table, row1);
    assertThat(DataHelper.valueReadFromDB(table, row1), IsNull.nullValue());
  }
}
