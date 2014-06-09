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
package org.apache.hadoop.hbase.client;

import c5db.client.FakeHTable;
import com.google.protobuf.Service;
import io.protostuff.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * The main client entry point for putting data into C5. Equivalent to HTablet from HBase.
 */
public class HTable extends FakeHTable implements HTableInterface {
  private static final Logger LOG = LoggerFactory.getLogger(HTable.class);

  public HTable(String hostname, int port, ByteString tableName) throws InterruptedException, TimeoutException, ExecutionException {
    super(hostname, port, tableName);
  }

  @Override
  public Result append(Append append) throws IOException {
    return null;
  }

  @Override
  public Result increment(Increment increment) throws IOException {
    return null;
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) throws IOException {
    return 0;
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, Durability durability) throws IOException {
    return 0;
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL) throws IOException {
    return 0;
  }

  @Override
  public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
    throw new IOException("We don't support getClosestRow");
  }


  @Override
  public CoprocessorRpcChannel coprocessorService(byte[] row) {
    LOG.error("Unspported");
    return null;
  }

  @Override
  public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service, byte[] startKey, byte[] endKey, Batch.Call<T, R> callable) throws Throwable {
    LOG.error("Unspported");
    return null;
  }

  @Override
  public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey, byte[] endKey, Batch.Call<T, R> callable, Batch.Callback<R> callback) throws Throwable {
    LOG.error("Unspported");
  }

  @Override
  public TableName getName() {
    return null;
  }

  public Configuration getConfiguration() {
    return null;
  }

  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    return null;
  }

  @Deprecated
  @Override
  public Object[] batch(List<? extends Row> actions) {
    throw new RuntimeException("We don't support these legacy operations");
  }

  @Deprecated
  @Override
  public <R> Object[] batchCallback(List<? extends Row> actions, Batch.Callback<R> callback) {
    throw new RuntimeException("We don't support these legacy operations");
  }

  @Override
  public <R> void batchCallback(List<? extends Row> actions, Object[] results, Batch.Callback<R> callback) {
    throw new RuntimeException("We don't support these legacy operations");
  }




}
