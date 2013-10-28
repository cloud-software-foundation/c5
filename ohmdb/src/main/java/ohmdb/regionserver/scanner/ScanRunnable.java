package ohmdb.regionserver.scanner;


import io.netty.channel.ChannelHandlerContext;
import ohmdb.client.OhmConstants;
import ohmdb.client.generated.ClientProtos;
import ohmdb.regionserver.ReverseProtobufUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.jetlang.core.Callback;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static ohmdb.OhmStatic.getOnlineRegion;

public class ScanRunnable implements Callback<Integer> {
    private final long scannerId;
    private final ClientProtos.Call call;
    private final ChannelHandlerContext ctx;
    private final RegionScanner scanner;
    private boolean close;

    public ScanRunnable(final ChannelHandlerContext ctx,
                        final ClientProtos.Call call,
                        final long scannerId) throws IOException {
        super();
        Scan scan = ReverseProtobufUtil.toScan(call.getScan().getScan());
        this.ctx = ctx;
        this.call = call;
        this.scannerId = scannerId;
        HRegion region = getOnlineRegion("1");
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
                    && rowsToSend < OhmConstants.MSG_SIZE
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


