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

import c5db.client.generated.GetRequest;
import c5db.client.generated.GetResponse;
import c5db.client.generated.MultiRequest;
import c5db.client.generated.MutateRequest;
import c5db.client.generated.RegionAction;
import c5db.client.generated.RegionSpecifier;
import c5db.client.generated.Response;
import c5db.client.generated.ScanRequest;
import c5db.client.scanner.ClientScannerManager;
import com.dyuproject.protostuff.ByteString;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.mortbay.log.Log;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;


/**
 * The main client entry point for putting data into C5. Equivalent to HTablet from HBase.
 */
public class C5Table extends C5Shim implements AutoCloseable {
  private final C5ConnectionManager c5ConnectionManager = new C5ConnectionManager();
  private final ClientScannerManager clientScannerManager = ClientScannerManager.INSTANCE;
  private final int port;
  private final AtomicLong commandId = new AtomicLong(0);
  private final Channel channel;
  private final String hostname;
  private MessageHandler handler;

  public C5Table(ByteString tableName) throws IOException, InterruptedException, TimeoutException, ExecutionException {
    this(tableName, C5Constants.TEST_PORT);
  }

  /**
   * C5Table is the main entry points for clients of C5DB
   *
   * @param tableName The name of the table to connect to.
   */
  public C5Table(ByteString tableName, int port)
      throws IOException, InterruptedException, TimeoutException, ExecutionException {
    super(tableName);
    this.hostname = "localhost";
    this.port = port;
    channel = c5ConnectionManager.getOrCreateChannel(this.hostname, this.port);
    handler = channel.pipeline().get(MessageHandler.class);
  }

  // TODO actually make this work
  public static ByteBuffer getRegion(byte[] row) {
    return ByteBuffer.wrap(new byte[]{});
  }

  @Override
  public Result get(final Get get) throws IOException {

    final SettableFuture<Response> resultFuture = SettableFuture.create();
    final GetRequest getRequest = RequestConverter.buildGetRequest(getRegionName(), get, false);
    try {
      handler.call(ProtobufUtil.getGetCall(commandId.incrementAndGet(), getRequest), resultFuture, channel);
      return ProtobufUtil.toResult(resultFuture.get(C5Constants.TIMEOUT, TimeUnit.MILLISECONDS).getGet().getResult());
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    //TODO Batch get
    final List<Result> results = new ArrayList<>();
    for (Get get : gets) {
      results.add(this.get(get));
    }
    return results.toArray(new Result[results.size()]);
  }

  @Override
  public boolean exists(final Get get) throws IOException {
    final SettableFuture<Response> resultFuture = SettableFuture.create();
    final GetRequest getRequest = RequestConverter.buildGetRequest(getRegionName(),
        get,
        true);

    try {
      handler.call(ProtobufUtil.getGetCall(commandId.incrementAndGet(), getRequest), resultFuture, channel);
      final GetResponse getResponse = resultFuture.get(C5Constants.TIMEOUT, TimeUnit.MILLISECONDS).getGet();
      final c5db.client.generated.Result result = getResponse.getResult();
      return result.getExists();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new IOException(e);
    }

  }

  @Override
  public ResultScanner getScanner(final Scan scan) throws IOException {
    if (scan.getStartRow() != null && scan.getStartRow().length > 0
        && scan.getStopRow() != null && scan.getStopRow().length > 0
        && Bytes.compareTo(scan.getStartRow(), scan.getStopRow()) > 0) {
      throw new IOException("StopRow needs to be greater than StartRow");
    }

    final SettableFuture<Long> future = SettableFuture.create();
    final RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        getRegion(scan.getStartRow()));

    final ScanRequest scanRequest = new ScanRequest(regionSpecifier,
        ProtobufUtil.toScan(scan),
        0L,
        C5Constants.DEFAULT_INIT_SCAN,
        false,
        0L);

    try {
      handler.callScan(ProtobufUtil.getScanCall(commandId.incrementAndGet(), scanRequest), future, channel);
      final long scannerId = future.get(C5Constants.TIMEOUT, TimeUnit.MILLISECONDS);
      /// TODO ADD A CREATE as well
      return clientScannerManager.get(scannerId);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new IOException(e);
    }
  }

  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    final Scan scan = new Scan();
    scan.addFamily(family);
    return getScanner(scan);
  }

  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
    final Scan scan = new Scan();
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
    final List<Boolean> results = new ArrayList<>();
    for (Get get : gets) {
      results.add(this.exists(get));
    }
    return results.toArray(new Boolean[results.size()]);
  }

  private void doPut(Put put) throws InterruptedIOException {
    final SettableFuture<Response> resultFuture = SettableFuture.create();
    final MutateRequest mutateRequest = RequestConverter.buildMutateRequest(getRegionName(), put);

    try {
      handler.call(ProtobufUtil.getMutateCall(commandId.incrementAndGet(), mutateRequest), resultFuture, channel);
      resultFuture.get(C5Constants.TIMEOUT, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
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
    final SettableFuture<Response> resultFuture = SettableFuture.create();
    final MutateRequest mutateRequest = RequestConverter.buildMutateRequest(getRegionName(), delete);

    try {
      handler.call(ProtobufUtil.getMutateCall(commandId.incrementAndGet(), mutateRequest), resultFuture, channel);
      resultFuture.get(C5Constants.TIMEOUT, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
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
    final SettableFuture<Response> resultFuture = SettableFuture.create();
    final List<RegionAction> regionActions = new ArrayList<>();
    try {
      final RegionAction regionAction = RequestConverter.buildRegionAction(getRegionName(), false, rm);
      regionActions.add(regionAction);

      handler.call(ProtobufUtil.getMultiCall(commandId.incrementAndGet(),
          new MultiRequest(regionActions)),
          resultFuture,
          channel);
      resultFuture.get(C5Constants.TIMEOUT, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | IOException | ExecutionException | TimeoutException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() {
    channel.writeAndFlush(new CloseWebSocketFrame());
    try {
      channel.closeFuture().sync();
      c5ConnectionManager.closeChannel(this.hostname, this.port);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      Log.warn(e);
    }

  }
}
