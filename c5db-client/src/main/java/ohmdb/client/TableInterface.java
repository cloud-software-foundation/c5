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
 *
 *  This file incorporates work covered by the following copyright and
 *  permission notice:
 */

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ohmdb.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

import java.io.IOException;
import java.util.List;

public interface TableInterface {

  public Configuration getConfiguration();

  public boolean exists(Get get) throws IOException;

  public Boolean[] exists(List<Get> gets) throws IOException;

  public void batch(List<? extends Row> actions, Object[] results) throws IOException, InterruptedException;

  public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException;

  public <R> void batchCallback(List<? extends Row> actions, Object[] results, Batch.Callback<R> callback) throws IOException, InterruptedException;

  public <R> Object[] batchCallback(List<? extends Row> actions, Batch.Callback<R> callback) throws IOException, InterruptedException;

  public Result get(Get get) throws IOException;

  public Result[] get(List<Get> gets) throws IOException;

  public ResultScanner getScanner(Scan scan) throws IOException;

  public ResultScanner getScanner(byte[] family) throws IOException;

  public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException;

  public void put(Put put) throws IOException;

  public void put(List<Put> puts) throws IOException;

  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException;

  public void delete(Delete delete) throws IOException;

  public void delete(List<Delete> deletes) throws IOException;

  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete) throws IOException;

  public void mutateRow(RowMutations rm) throws IOException;

  public void close() throws IOException;
}
