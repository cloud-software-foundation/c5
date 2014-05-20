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
 *
 *  This file incorporates work covered by the following copyright and
 *  permission notice:
 */
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;

import java.io.IOException;

public interface HRegionInterface {
  void delete(Delete delete) throws IOException;

  boolean checkAndMutate(byte[] row,
                         byte[] cf,
                         byte[] cq,
                         CompareFilter.CompareOp compareOp,
                         ByteArrayComparable comparator,
                         Mutation m,
                         boolean b) throws IOException;

  void put(Put put) throws IOException;

  Result get(Get get) throws IOException;

  RegionScanner getScanner(Scan scan) throws IOException;

  void mutateRow(RowMutations rm) throws IOException;

  void processRowsWithLocks(RowProcessor<?, ?> processor) throws IOException;

}
