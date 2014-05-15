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

/** Incorporates changes licensed under:
 *
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

package c5db.client.scanner;

import c5db.client.C5Constants;
import c5db.client.ProtobufUtil;
import c5db.client.RequestConverter;
import c5db.client.generated.RegionSpecifier;
import c5db.client.generated.ScanRequest;
import c5db.client.generated.ScanResponse;
import c5db.client.queue.WickedQueue;
import io.netty.channel.Channel;
import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.ArrayList;

public class ClientScanner extends AbstractClientScanner {
  private final Channel ch;
  private final long scannerId;
  private final WickedQueue<c5db.client.generated.Result> scanResults = new WickedQueue<>(C5Constants.MAX_CACHE_SZ);
  private final long commandId;
  private boolean isClosed = true;


  private int requestSize = C5Constants.DEFAULT_INIT_SCAN;
  private int outStandingRequests = C5Constants.DEFAULT_INIT_SCAN;


  /**
   * Create a new ClientScanner for the specified table
   * Note that the passed {@link org.apache.hadoop.hbase.client.Scan}'s start row maybe changed changed.
   */
  ClientScanner(Channel channel, final long scannerId, final long commandId) {
    ch = channel;
    this.scannerId = scannerId;
    this.commandId = commandId;
    this.isClosed = false;
  }

  @Override
  public Result next() throws IOException {

    if (this.isClosed && this.scanResults.isEmpty()) {
      return null;
    }

    c5db.client.generated.Result result;
    do {
      result = scanResults.poll();

      if (!this.isClosed) {
        // If we don't have enough pending outstanding increase our rate
        if (this.outStandingRequests < .5 * requestSize && requestSize < C5Constants.MAX_REQUEST_SIZE) {
          requestSize = requestSize * 2;
        }
        final int queueSpace = C5Constants.MAX_CACHE_SZ - this.scanResults.size();

        // If we have plenty of room for another request
        if (queueSpace * 1.5 > (requestSize + this.outStandingRequests)
            // And we have less than two requests worth in the queue
            && 2 * this.outStandingRequests < requestSize) {
          getMoreRows();
        }
      }
    } while (result == null && !this.isClosed);

    if (result == null) {
      return null;
    }

    return ProtobufUtil.toResult(result);
  }

  private void getMoreRows() throws IOException {
    //TODO getRegion shouldn't be needed and currently is hardcoded
    final RegionSpecifier regionSpecifier = RequestConverter.buildRegionSpecifier(new byte[]{});

    final ScanRequest scanRequest = new ScanRequest(regionSpecifier, null, scannerId, requestSize, false, 0);
    this.outStandingRequests += requestSize;
    ch.write(ProtobufUtil.getScanCall(commandId, scanRequest));
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
    this.isClosed = true;
  }

  public void add(ScanResponse response) {
    for (c5db.client.generated.Result result : response.getResultsList()) {
      scanResults.add(result);
      this.outStandingRequests--;
    }
  }
}
