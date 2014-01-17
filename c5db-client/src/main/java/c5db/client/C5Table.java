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

package c5db.client;

import c5db.ProtobufUtil;
import c5db.RequestConverter;
import c5db.client.generated.ClientProtos;
import c5db.client.generated.HBaseProtos;
import c5db.client.scanner.ClientScannerManager;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import io.netty.channel.Channel;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;


public class C5Table extends C5Shim implements AutoCloseable {
    private final C5ConnectionManager c5ConnectionManager
            = C5ConnectionManager.INSTANCE;
    private final ClientScannerManager clientScannerManager
            = ClientScannerManager.INSTANCE;
    private RequestHandler handler;
    private AtomicLong commandId = new AtomicLong(0);
    private Channel channel;

    public C5Table(ByteString tableName) throws IOException, InterruptedException {
        this(tableName, C5Constants.TEST_PORT);
    }
    /**
     * C5Table is the main entry points for clients of C5DB
     *
     * @param tableName The name of the table to connect to.
     */
    public C5Table(ByteString tableName, int port) throws IOException, InterruptedException {
        super(tableName);

        channel = c5ConnectionManager.getOrCreateChannel("localhost", port);

        handler = channel
                .pipeline()
                .get(RequestHandler.class);
    }

    @Override
    public Result get(final Get get) throws IOException {
        final SettableFuture<ClientProtos.Response> resultFuture
                = SettableFuture.create();
        final ClientProtos.Call.Builder call = ClientProtos.Call.newBuilder();
        final ClientProtos.GetRequest getRequest =
                RequestConverter.buildGetRequest(getRegionName(),
                        get,
                        false);
        call.setGet(getRequest);
        call.setCommand(ClientProtos.Call.Command.GET);
        call.setCommandId(commandId.incrementAndGet());

        try {
            handler.call(call.build(), resultFuture, channel);
            return ProtobufUtil.toResult(resultFuture.get().getGet().getResult());
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Result[] get(List<Get> gets) throws IOException {
        //TODO Batch get
        List<Result> results = new ArrayList<>();
        for (Get get: gets){
            results.add(this.get(get));
        }
        return results.toArray(new Result[results.size()]);
    }

    @Override
    public boolean exists(final Get get) throws IOException {
        final SettableFuture<ClientProtos.Response> resultFuture
                = SettableFuture.create();
        final ClientProtos.Call.Builder call = ClientProtos.Call.newBuilder();
        final ClientProtos.GetRequest getRequest =
                RequestConverter.buildGetRequest(getRegionName(),
                        get,
                        true);
        call.setGet(getRequest);
        call.setCommand(ClientProtos.Call.Command.GET);
        call.setCommandId(commandId.incrementAndGet());

        try {
            handler.call(call.build(), resultFuture, channel);
            return resultFuture.get().getGet().getResult().getExists();
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException(e);
        }

    }

    @Override
    public ResultScanner getScanner(final Scan scan) throws IOException {
        if (scan.getStartRow() != null && scan.getStartRow().length > 0) {
            if (scan.getStopRow() != null && scan.getStopRow().length > 0) {
                if (Bytes.compareTo(scan.getStartRow(), scan.getStopRow()) > 0) {
                    throw new IOException("StopRow needs to be greater than StartRow");
                }
            }
        }
        SettableFuture<Long> future = SettableFuture.create();
        HBaseProtos.RegionSpecifier regionSpecifier =
                HBaseProtos.RegionSpecifier.newBuilder()
                        .setType(HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME)
                        .setValue(ByteString.copyFromUtf8("value")).build();
        ClientProtos.ScanRequest scanRequest = ClientProtos.ScanRequest.newBuilder()
                .setScan(ProtobufUtil.toScan(scan))
                .setRegion(regionSpecifier)
                .setNumberOfRows(C5Constants.DEFAULT_INIT_SCAN)
                .build();

        ClientProtos.Call call = ClientProtos.Call.newBuilder()
                .setCommand(ClientProtos.Call.Command.SCAN)
                .setCommandId(commandId.incrementAndGet())
                .setScan(scanRequest)

                .build();

        try {
            handler.call(call, future, channel);
            Long scannerId = future.get();
            return clientScannerManager.getOrCreate(scannerId);
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException(e);
        }
    }

    @Override
    public ResultScanner getScanner(byte[] family) throws IOException {
        Scan scan = new Scan();
        scan.addFamily(family);
        return getScanner(scan);
    }

    @Override
    public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
        Scan scan = new Scan();
        scan.addColumn(family, qualifier);
        return getScanner(scan);
    }

    @Override
    public void put(Put put) throws IOException {
       doPut(put);
    }

    @Override
    public Boolean[] exists(List<Get> gets) throws IOException {
        //TODO Batch get
        List<Boolean> results = new ArrayList<>();
        for (Get get: gets){
            results.add(this.exists(get));
        }
        return results.toArray(new Boolean[results.size()]);
    }

    private void doPut(Put put) throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        ClientProtos.Call.Builder call = ClientProtos.Call.newBuilder();
        ClientProtos.MutateRequest mutateRequest =
                null;
        try {
            mutateRequest = RequestConverter.buildMutateRequest(getRegionName(), put);
        } catch (IOException e) {
            throw new InterruptedIOException(e.getLocalizedMessage());
        }
        call.setMutate(mutateRequest);
        call.setCommand(ClientProtos.Call.Command.MUTATE);
        call.setCommandId(commandId.incrementAndGet());
        final SettableFuture<ClientProtos.Response> resultFuture
                = SettableFuture.create();

        try {
            handler.call(call.build(), resultFuture, channel);
            resultFuture.get();
        } catch (InterruptedException | IOException | ExecutionException e) {
            throw new InterruptedIOException(e.toString());
        }


    }


    @Override
    public void put(List<Put> puts) throws IOException {
        for (Put put : puts) {
            doPut(put);
        }
    }

    @Override
    public void delete(Delete delete) throws IOException {
        ClientProtos.Call.Builder call = ClientProtos.Call.newBuilder();
        ClientProtos.MutateRequest mutateRequest =
                RequestConverter.buildMutateRequest(getRegionName(), delete);
        call.setMutate(mutateRequest);
        call.setCommand(ClientProtos.Call.Command.MUTATE);
        call.setCommandId(commandId.incrementAndGet());
        final SettableFuture<ClientProtos.Response> resultFuture
                = SettableFuture.create();

        try {
            handler.call(call.build(), resultFuture, channel);
            resultFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void delete(List<Delete> deletes) throws IOException {
        for (Delete delete : deletes) {
            delete(delete);
        }
    }

    @Override
    public void mutateRow(RowMutations rm) throws IOException {
        ClientProtos.Call.Builder call = ClientProtos.Call.newBuilder();
        try {
            ClientProtos.RegionAction.Builder regionMutationBuilder =
                    RequestConverter.buildRegionAction(getRegionName(), rm);
            ClientProtos.MultiRequest.Builder multiRequest = ClientProtos
                    .MultiRequest
                    .newBuilder()
                    .addRegionAction(regionMutationBuilder.build());
            call.setMulti(multiRequest.build());
            call.setCommand(ClientProtos.Call.Command.MULTI);
            call.setCommandId(commandId.incrementAndGet());
            final SettableFuture<ClientProtos.Response> resultFuture
                    = SettableFuture.create();

            handler.call(call.build(), resultFuture, channel);
            resultFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        //Currently if not called then we can't close the jvm.
        try {
            c5ConnectionManager.close();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

}
