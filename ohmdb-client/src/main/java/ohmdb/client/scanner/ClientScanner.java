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


package ohmdb.client.scanner;

import com.google.protobuf.ByteString;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import ohmdb.ProtobufUtil;
import ohmdb.client.OhmConnectionManager;
import ohmdb.client.OhmConstants;
import ohmdb.client.RequestHandler;
import ohmdb.client.generated.ClientProtos;
import ohmdb.client.generated.HBaseProtos;
import ohmdb.client.queue.WickedQueue;
import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.ArrayList;

public class ClientScanner extends AbstractClientScanner {
  final RequestHandler handler;
  private final Channel ch;
  private final long scannerId;
  private final WickedQueue<ClientProtos.Result>
      scanResults = new WickedQueue<>(OhmConstants.MAX_CACHE_SZ);
  private boolean isClosed = true;
  private final OhmConnectionManager ohmConnectionManager
      = OhmConnectionManager.INSTANCE;
  private int outStandingRequests = OhmConstants.DEFAULT_INIT_SCAN;
  private int requestSize = OhmConstants.DEFAULT_INIT_SCAN;

  /**
   * Create a new ClientScanner for the specified table
   * Note that the passed {@link Scan}'s start row maybe changed changed.
   *
   * @throws IOException
   */
  public ClientScanner(final long scannerId) throws IOException {
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
    this.isClosed = false;
  }

  @Override
  public Result next() throws IOException {
    if (this.isClosed && this.scanResults.isEmpty()) {
      return null;
    }

    ClientProtos.Result result;
    do {
      result = scanResults.poll();

      if (!this.isClosed) {
        // If we don't have enough pending outstanding increase our rate
        if (this.outStandingRequests < .5 * requestSize ) {
          if (requestSize < OhmConstants.MAX_REQUEST_SIZE) {
            requestSize = requestSize * 2;
            System.out.println("increasing requestSize:" + requestSize);
            System.out.flush();
          }
        }
        int queueSpace = OhmConstants.MAX_CACHE_SZ - this.scanResults.size();

        // If we have plenty of room for another request
        if (queueSpace * 1.5  >  (requestSize + this.outStandingRequests)
          // And we have less than two requests worth in the queue
           &&  2 * this.outStandingRequests < requestSize) {
          getMoreRows();
        }
      }
    } while (result == null && !this.isClosed);

    if (result == null){
      return null;
    }

    return ProtobufUtil.toResult(result);
  }

  private void getMoreRows() throws IOException {
    HBaseProtos.RegionSpecifier regionSpecifier =
        HBaseProtos.RegionSpecifier.newBuilder()
            .setType(HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME)
            .setValue(ByteString.copyFromUtf8("value")).build();

    ClientProtos.ScanRequest.Builder scanRequest = ClientProtos.ScanRequest.newBuilder()
        .setScannerId(scannerId)
        .setRegion(regionSpecifier);
        scanRequest.setNumberOfRows(requestSize);

    ClientProtos.Call call = ClientProtos.Call.newBuilder()
        .setCommand(ClientProtos.Call.Command.SCAN)
        .setCommandId(0)
        .setScan(scanRequest)
        .build();

    try {
      Channel channel = ohmConnectionManager
          .getOrCreateChannel("localhost", OhmConstants.TEST_PORT);
      this.outStandingRequests += requestSize;
      channel.write(call);

    } catch (InterruptedException e) {
      throw new IOException(e);
    }
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
    this.isClosed = true;
  }

  public void add(ClientProtos.ScanResponse response) {
    for (ClientProtos.Result result : response.getResultList()) {
      scanResults.add(result);
      this.outStandingRequests--;
    }
  }
}
