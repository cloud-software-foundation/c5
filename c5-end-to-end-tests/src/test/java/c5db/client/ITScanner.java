/*
 * Copyright (C) 2014  Ohm Data
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

import c5db.ClusterPopulated;
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

@Ignore
public class ITScanner extends ClusterPopulated {

  @Ignore // Pending resolution of issue #304
  @Test
  public void scan() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    int i = 0;
    Result result;

    ResultScanner scanner = table.getScanner(new Scan().setStartRow(new byte[]{0x00}));
    do {
      result = scanner.next();
      if (result != null) {
        i++;
      }
    } while (result != null);
    scanner.close();
    assertThat(i, is(this.NUMBER_OF_ROWS));
  }
}