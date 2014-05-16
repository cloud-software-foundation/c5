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

import io.protostuff.ByteString;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;


/**
 * The main client entry point for putting data into C5. Equivalent to HTablet from HBase.
 */
public class FakeHTable extends c5db.client.FakeHTable {
  public FakeHTable(String hostname, int port, ByteString tableName) throws IOException, InterruptedException, TimeoutException, ExecutionException {
    super(hostname, port, tableName);
  }
}
