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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

import java.io.IOException;
import java.util.List;

/**
 * A shim for unsupported options to allow C5Table to implement TableInterface
 */
public abstract class C5Shim implements TableInterface {

  @Override
  public Configuration getConfiguration() {
    return null;
  }

  @Override
  public void batch(List<? extends Row> actions, Object[] results) throws IOException, InterruptedException {
  }

  @Override
  public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
    return new Object[0];
  }

  @Override
  public <R> void batchCallback(List<? extends Row> actions, Object[] results, Batch.Callback<R> callback)
      throws IOException, InterruptedException {
  }

  @Override
  public <R> Object[] batchCallback(List<? extends Row> actions, Batch.Callback<R> callback)
      throws IOException, InterruptedException {
    return new Object[0];
  }

}
