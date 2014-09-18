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
 */
package c5db.client;

import c5db.ClusterOrPseudoCluster;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.core.IsNull;
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
