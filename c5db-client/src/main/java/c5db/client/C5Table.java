
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
import c5db.client.generated.Call;
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
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.mortbay.log.Log;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;


public class C5Table extends C5Shim implements AutoCloseable {
  private final C5ConnectionManager c5ConnectionManager = new C5ConnectionManager();
  private final ClientScannerManager clientScannerManager = ClientScannerManager.INSTANCE;
  private final int port;
  private MessageHandler handler;
  private AtomicLong commandId = new AtomicLong(0);
  private Channel channel;
  private String hostname;

  public C5Table(ByteString tableName) throws IOException, InterruptedException, TimeoutException, ExecutionException {
    this(tableName, C5Constants.TEST_PORT);
  }

  /**
   * C5Table is the main entry points for clients of C5DB
   *
   * @param tableName The name of the table to connect to.
   */
  public C5Table(ByteString tableName, int port) throws IOException, InterruptedException, TimeoutException, ExecutionException {
    super(tableName);
    this.hostname = "localhost";
    this.port = port;
    channel = c5ConnectionManager.getOrCreateChannel(this.hostname, this.port);
    handler = channel.pipeline().get(MessageHandler.class);
  }

  @Override
  public Result get(final Get get) throws IOException {

    final SettableFuture<Response> resultFuture
        = SettableFuture.create();
    final Call call = new Call();
    final GetRequest getRequest =
        RequestConverter.buildGetRequest(getRegionName(),
            get,
            false);
    call.setGet(getRequest);
    call.setCommand(Call.Command.GET);
    call.setCommandId(commandId.incrementAndGet());

    try {
      handler.call(call, resultFuture, channel);
      return ProtobufUtil.toResult(resultFuture.get(C5Constants.TIMEOUT, TimeUnit.MILLISECONDS).getGet().getResult());
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    //TODO Batch get
    List<Result> results = new ArrayList<>();
    for (Get get : gets) {
      results.add(this.get(get));
    }
    return results.toArray(new Result[results.size()]);
  }

  @Override
  public boolean exists(final Get get) throws IOException {

    final SettableFuture<Response> resultFuture = SettableFuture.create();
    final Call call = new Call();
    final GetRequest getRequest = RequestConverter.buildGetRequest(getRegionName(),
        get,
        true);
    call.setGet(getRequest);
    call.setCommand(Call.Command.GET);
    call.setCommandId(commandId.incrementAndGet());

    try {
      handler.call(call, resultFuture, channel);
      GetResponse getResponse = resultFuture.get(C5Constants.TIMEOUT, TimeUnit.MILLISECONDS).getGet();
      c5db.client.generated.Result result = getResponse.getResult();
      return result.getExists();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
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
    RegionSpecifier regionSpecifier = new RegionSpecifier()
        .setType(RegionSpecifier.RegionSpecifierType.REGION_NAME)
        .setValue(ByteString.copyFromUtf8("value"));
    ScanRequest scanRequest = new ScanRequest();
    scanRequest.setScan(ProtobufUtil.toScan(scan));
    scanRequest.setRegion(regionSpecifier);
    scanRequest.setNumberOfRows(C5Constants.DEFAULT_INIT_SCAN);

    Call call = new Call()
        .setCommand(Call.Command.SCAN)
        .setCommandId(commandId.incrementAndGet())
        .setScan(scanRequest);


    try {
      handler.call(call, future, channel);
      Long scannerId = future.get(C5Constants.TIMEOUT, TimeUnit.MILLISECONDS);
      /// TODO ADD A CREATE as well
      return clientScannerManager.get(scannerId);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
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
    for (Get get : gets) {
      results.add(this.exists(get));
    }
    return results.toArray(new Boolean[results.size()]);
  }

  private void doPut(Put put) throws InterruptedIOException, RetriesExhaustedWithDetailsException {

    final SettableFuture<Response> resultFuture
        = SettableFuture.create();

    Call call = new Call();
    MutateRequest mutateRequest;
    try {
      mutateRequest = RequestConverter.buildMutateRequest(getRegionName(), put);
    } catch (IOException e) {
      throw new InterruptedIOException(e.getLocalizedMessage());
    }
    call.setMutate(mutateRequest);
    call.setCommand(Call.Command.MUTATE);
    call.setCommandId(commandId.incrementAndGet());

    try {
      handler.call(call, resultFuture, channel);
      resultFuture.get(C5Constants.TIMEOUT, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | IOException | ExecutionException | TimeoutException e) {
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

    final SettableFuture<Response> resultFuture
        = SettableFuture.create();

    Call call = new Call();
    MutateRequest mutateRequest =
        RequestConverter.buildMutateRequest(getRegionName(), delete);
    call.setMutate(mutateRequest);
    call.setCommand(Call.Command.MUTATE);
    call.setCommandId(commandId.incrementAndGet());

    try {
      handler.call(call, resultFuture, channel);
      resultFuture.get(C5Constants.TIMEOUT, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | IOException | ExecutionException | TimeoutException e) {
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

    final SettableFuture<Response> resultFuture
        = SettableFuture.create();

    Call call = new Call();
    try {
      RegionAction regionAction = RequestConverter.buildRegionAction(getRegionName(), rm);
      MultiRequest multiRequest = new MultiRequest();
      addRegionAction(multiRequest, regionAction);

      call.setMulti(multiRequest);
      call.setCommand(Call.Command.MULTI);
      call.setCommandId(commandId.incrementAndGet());

      handler.call(call, resultFuture, channel);
      resultFuture.get(C5Constants.TIMEOUT, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | IOException | ExecutionException | TimeoutException e) {
      throw new IOException(e);
    }
  }

  private void addRegionAction(MultiRequest multiRequest, RegionAction regionAction) {
    List<RegionAction> regionActionList = multiRequest.getRegionActionList();
    regionActionList.add(regionAction);
    multiRequest.setRegionActionList(regionActionList);
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
