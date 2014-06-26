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
 */

package c5db.regionserver.scan;


import c5db.client.generated.Call;
import c5db.client.generated.LocationResponse;
import c5db.client.generated.Response;
import c5db.client.generated.Result;
import c5db.client.generated.ScanResponse;
import c5db.regionserver.ReverseProtobufUtil;
import c5db.tablet.Region;
import io.netty.channel.ChannelHandlerContext;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.jetbrains.annotations.NotNull;
import org.jetlang.core.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Creates a runnable in the background so that the regionserver always has a setup of scanner results
 * ready to send back to the user. It directly sends the data back through netty back to the user.
 */
public class ScanRunnable implements Callback<Integer> {
  private static final int MAX_ROWS_TO_SEND_IN_A_SCAN_RESPONSE = 100;
  private final long scannerId;
  private final Call call;
  private final ChannelHandlerContext ctx;

  private final RegionScanner scanner;
  private boolean toSendClose = false;
  long count = 0;
  private LocationResponse nextLocation;
  private boolean close;
  private static final Logger LOG = LoggerFactory.getLogger(ScanRunnable.class);

  public ScanRunnable(final ChannelHandlerContext ctx,
                      final Call call,
                      final long scannerId,
                      final Region region,
                      final LocationResponse nextLocation) throws IOException {
    super();
    assert (call.getScan() != null);
    this.ctx = ctx;
    this.call = call;
    this.scannerId = scannerId;
    this.close = false;
    this.scanner = region.getScanner(call.getScan().getScan());
    this.nextLocation = nextLocation;
  }

  @Override
  public void onMessage(Integer numberOfMessagesToSend) {
    if (this.close) {
      return;
    }

    long numberOfMessagesLeftToSend = numberOfMessagesToSend;
    try {

      while (!this.close && numberOfMessagesLeftToSend > 0) {
        List<Integer> cellsPerResult = new ArrayList<>();
        List<Result> scanResults = new ArrayList<>();
        int rowBufferedToSend = 0;

        while (!this.close &&
            rowBufferedToSend < MAX_ROWS_TO_SEND_IN_A_SCAN_RESPONSE &&
            numberOfMessagesToSend - rowBufferedToSend > 0) {
          Result result = getNextRow(scanner);
          rowBufferedToSend++;
          scanResults.add(result);
          if (result != null) {
            cellsPerResult.add(result.getCellList().size());
          }
        }

        if (scanResults.size() > 1 || this.nextLocation != null || toSendClose) {
          numberOfMessagesLeftToSend = sendScannerResponse(numberOfMessagesLeftToSend,
              cellsPerResult,
              scanResults,
              rowBufferedToSend,
              !this.close);
        } else {

        }
      }
    } catch (IOException e) {
      LOG.error("Error closing scanner:" + e.getLocalizedMessage());
      e.printStackTrace();
    }

  }

  private Result getNextRow(@NotNull RegionScanner scanner) throws IOException {
    boolean moreResults;
    List<Cell> rawCells = new ArrayList<>();
    // Get a row worth of results
    moreResults = scanner.next(rawCells);
    List<c5db.client.generated.Cell> protoCells = rawCells
        .stream()
        .map(ReverseProtobufUtil::toCell)
        .collect(Collectors.toList());

    if (!moreResults) {
      scanner.close();
      this.close = true;
      this.toSendClose = true;
    }
    return new Result(protoCells, protoCells.size(), protoCells.size() > 0);
  }

  private long sendScannerResponse(long numberOfMessagesLeftToSend,
                                   List<Integer> cellsPerResult,
                                   List<Result> scanResults,
                                   int rowBufferedToSend,
                                   boolean moreResults) {
    ScanResponse scanResponse = new ScanResponse(cellsPerResult, scannerId, moreResults, 0, scanResults);
    Response response = new Response(Response.Command.SCAN, call.getCommandId(), null, null, scanResponse, null,
        this.nextLocation);

    // only send the next location with the first response.
    if (this.nextLocation != null){
      this.nextLocation = null;
    }

    ctx.writeAndFlush(response);
    if (toSendClose && !moreResults){
      toSendClose = false;
    }

    numberOfMessagesLeftToSend -= rowBufferedToSend;
    return numberOfMessagesLeftToSend;
  }
}