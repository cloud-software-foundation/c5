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
package c5db.regionserver.scanner;


import c5db.client.C5Constants;
import c5db.client.generated.ClientProtos;
import c5db.regionserver.ReverseProtobufUtil;
import io.netty.channel.ChannelHandlerContext;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.jetlang.core.Callback;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ScanRunnable implements Callback<Integer> {
    private final long scannerId;
    private final ClientProtos.Call call;
    private final ChannelHandlerContext ctx;
    private final RegionScanner scanner;
    private boolean close;

    public ScanRunnable(final ChannelHandlerContext ctx,
                        final ClientProtos.Call call,
                        final long scannerId, HRegion region) throws IOException {
        super();
        Scan scan = ReverseProtobufUtil.toScan(call.getScan().getScan());
        this.ctx = ctx;
        this.call = call;
        this.scannerId = scannerId;
        this.scanner = region.getScanner(scan);
        this.close = false;
    }

    @Override
    public void onMessage(Integer numberOfMessagesToSend) {
        if (this.close) {
            return;
        }
        long numberOfMsgsLeft = numberOfMessagesToSend;
        while (!this.close && numberOfMsgsLeft > 0) {
            ClientProtos.ScanResponse.Builder scanResponse
                    = ClientProtos.ScanResponse.newBuilder();
            scanResponse.setScannerId(scannerId);

            int rowsToSend = 0;
            boolean moreResults;
            do {
                List<Cell> kvs = new ArrayList<>();

                try {
                    moreResults = scanner.nextRaw(kvs);
                    if (!moreResults) {
                        this.scanner.close();
                        this.close = true;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }

                ClientProtos.Result.Builder resultBuilder =
                        ClientProtos.Result.newBuilder();

                for (Cell kv : kvs) {
                    resultBuilder.addCell(ReverseProtobufUtil.toCell(kv));
                }
                scanResponse.addResults(resultBuilder.build());
                rowsToSend++;

            } while (moreResults
                    && rowsToSend < C5Constants.MSG_SIZE
                    && numberOfMessagesToSend - rowsToSend > 0);
            scanResponse.setMoreResults(moreResults);
            ClientProtos.Response response = ClientProtos
                    .Response
                    .newBuilder()
                    .setCommand(ClientProtos.Response.Command.SCAN)
                    .setCommandId(call.getCommandId())
                    .setScan(scanResponse.build()).build();

            ctx.writeAndFlush(response);
            numberOfMsgsLeft -= rowsToSend;
        }
    }
}


