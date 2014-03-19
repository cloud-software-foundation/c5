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


import com.dyuproject.protostuff.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

import java.io.IOException;
import java.util.List;

public abstract class C5Shim implements TableInterface {
  private final byte[] tableName;

  /**
   * C5Table is the main entry points for clients of C5DB
   *
   * @param tableName The tablename to create a table for.
   */
  C5Shim(final ByteString tableName) {
    this.tableName = tableName.toByteArray();
  }

  public byte[] getTableName() {
    return tableName;
  }

  byte[] getRegionName() {
    return new byte[]{0, 1, 2};
  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete) throws IOException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Configuration getConfiguration() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }


  @Override
  public void batch(List<? extends Row> actions, Object[] results) throws IOException, InterruptedException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
    return new Object[0];  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public <R> void batchCallback(List<? extends Row> actions, Object[] results, Batch.Callback<R> callback) throws IOException, InterruptedException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public <R> Object[] batchCallback(List<? extends Row> actions, Batch.Callback<R> callback) throws IOException, InterruptedException {
    return new Object[0];  //To change body of implemented methods use File | Settings | File Templates.
  }

}
