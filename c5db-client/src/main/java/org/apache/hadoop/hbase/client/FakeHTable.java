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
public class FakeHTable extends c5db.client.FakeHTable implements HTableInterface {
  private static final Logger LOG = LoggerFactory.getLogger(FakeHTable.class);

  public FakeHTable(String hostname, int port, ByteString tableName) throws InterruptedException, TimeoutException, ExecutionException {
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
  public boolean isAutoFlush() {
    LOG.error("we auto flush by default");
    return true;
  }

  @Override
  public void setAutoFlush(boolean autoFlush) {
    LOG.error("Unspported");

  }

  @Override
  public void flushCommits() throws IOException {
    LOG.error("we auto flush by default");
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
  public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
    LOG.error("Unspported");
  }

  @Override
  public void setAutoFlushTo(boolean autoFlush) {
    LOG.error("Unspported");
  }

  @Override
  public long getWriteBufferSize() {
    LOG.error("Unspported");
    return 0;
  }

  @Override
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
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

  public void batch(List<? extends Row> actions, Object[] results) {
  }

  public Object[] batch(List<? extends Row> actions) {
    return new Object[0];
  }

  public <R> void batchCallback(List<? extends Row> actions, Object[] results, Batch.Callback<R> callback) {
  }

  public <R> Object[] batchCallback(List<? extends Row> actions, Batch.Callback<R> callback) {
    return new Object[0];
  }


}
