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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import ohmdb.ProtobufUtil;
import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.ArrayList;

public class ClientScanner extends AbstractClientScanner {
  final RequestHandler handler;
  private final Channel ch;
  private final long scannerId;

  /**
   * Create a new ClientScanner for the specified table
   * Note that the passed {@link Scan}'s start row maybe changed changed.
   *
   *
   * @param scan      {@link org.apache.hadoop.hbase.client.Scan} to use in this scanner
   * @param tableName The table that we wish to scan
   * @throws IOException
   */
  public ClientScanner(final Scan scan,
                       final byte[] tableName,
                       final long scannerId) throws IOException {

    OhmConnectionManager ohmConnectionManager = OhmConnectionManager.INSTANCE;
    try {
      ch = ohmConnectionManager.getOrCreateChannel("localhost",
          OhmConstants.TEST_PORT);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    final ChannelPipeline pipeline = ch.pipeline();
    handler = pipeline.get(RequestHandler.class);
    this.scannerId = scannerId;
  }

  @Override
  public Result next() throws IOException {
    if (handler.isClosed(scannerId)) {
      return null;
    }
    return ProtobufUtil.toResult(handler.next(scannerId));
  }

  @Override
  public Result[] next(final int nbRows) throws IOException {
    final ArrayList<Result> resultSets = new ArrayList<>(nbRows);
    for (int i = 0; i < nbRows; i++) {
      final Result next = next();
      if (next != null) {
        resultSets.add(next);
      } else {
        break;
      }
    }
    return resultSets.toArray(new Result[resultSets.size()]);
  }

  @Override
  public void close() {
    ChannelFuture f = ch.close();
    try {
      f.sync();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
