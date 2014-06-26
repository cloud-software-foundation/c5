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

import c5db.MiniClusterPopulated;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;


public class TestScannerTest extends ManyClusterPopulated {
  private static final Logger LOG = LoggerFactory.getLogger(TestScannerTest.class);

  int SCANNER_TRIALS = 10;

  @Test
  public void scan() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    for (int j = 0; j != SCANNER_TRIALS; j++) {
      LOG.info("starting scanner trial:" + j);
      int i = 0;
      Result result;

      ResultScanner scanner = table.getScanner(new Scan());
      int previous_row = -1;

      do {
        result = scanner.next();
        if (result != null) {
          int row = Bytes.toInt(result.getRow());
          assertThat(row, is(previous_row + 1));
          previous_row = row;
          i++;
        }
      } while (result != null);
      scanner.close();
      assertThat(i, is(this.NUMBER_OF_ROWS));
    }
  }


}