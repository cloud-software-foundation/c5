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

import c5db.client.generated.ByteArrayComparable;
import c5db.client.generated.CompareType;
import c5db.client.generated.Condition;
import c5db.client.generated.GetRequest;
import c5db.client.generated.GetResponse;
import c5db.client.generated.MultiRequest;
import c5db.client.generated.MutateRequest;
import c5db.client.generated.MutationProto;
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
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
  private static final Logger LOG = LoggerFactory.getLogger(C5Table.class);
  private final C5ConnectionManager c5ConnectionManager = new C5ConnectionManager();
  private final ClientScannerManager clientScannerManager = ClientScannerManager.INSTANCE;
  private final int port;
  private final AtomicLong commandId = new AtomicLong(0);
  private final Channel channel;
  private final String hostname;
  private final byte[] tableName;
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
    this.tableName = tableName.toByteArray();
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
    try {
      return doGetResult(get);
    } catch (RuntimeException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    //TODO Batch get
    try {
      return gets.stream().map(this::doGetResult).toArray(Result[]::new);
    } catch (RuntimeException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean exists(final Get get) throws IOException {
    try {
      return doExists(get);
    } catch (RuntimeException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Boolean[] exists(List<Get> gets) throws IOException {
    try {
      return gets.stream().map(this::doExists).toArray(Boolean[]::new);
    } catch (RuntimeException e) {
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
    try {
      doPut(put);
    } catch (RuntimeException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void delete(Delete delete) throws IOException {
    try {
      doDelete(delete);
    } catch (RuntimeException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void put(List<Put> puts) throws IOException {

    try {
      puts.forEach(this::doPut);
    } catch (RuntimeException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void delete(List<Delete> deletes) throws IOException {
    try {
      deletes.forEach(this::doDelete);
    } catch (RuntimeException e) {
      throw new IOException(e);
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
          channel
      );
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
      LOG.error(e.toString());
    }
  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put)
      throws IOException {
    final SettableFuture<Response> resultFuture = SettableFuture.create();
    ByteArrayComparable byteArrayComparable = new ByteArrayComparable(ByteBuffer.wrap(value));

    final Condition condition = new Condition(ByteBuffer.wrap(row),
        ByteBuffer.wrap(family),
        ByteBuffer.wrap(qualifier),
        CompareType.EQUAL,
        ProtobufUtil.toComparator(byteArrayComparable));

    MutateRequest mutateRequest = RequestConverter.buildMutateRequest(getRegionName(),
        MutationProto.MutationType.PUT,
        put,
        condition);

    try {
      handler.call(ProtobufUtil.getMutateCall(commandId.incrementAndGet(), mutateRequest), resultFuture, channel);
      Response result = resultFuture.get(C5Constants.TIMEOUT, TimeUnit.MILLISECONDS);
      if (result.getMutate().getProcessed()) {
        return true;
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new IOException(e.toString());
    }
    return false;
  }

  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete)
      throws IOException {
    final SettableFuture<Response> resultFuture = SettableFuture.create();
    ByteArrayComparable byteArrayComparable = new ByteArrayComparable(ByteBuffer.wrap(value));

    final Condition condition = new Condition(ByteBuffer.wrap(row),
        ByteBuffer.wrap(family),
        ByteBuffer.wrap(qualifier),
        CompareType.EQUAL,
        ProtobufUtil.toComparator(byteArrayComparable));

    MutateRequest mutateRequest = RequestConverter.buildMutateRequest(getRegionName(),
        MutationProto.MutationType.DELETE,
        delete,
        condition);
    try {
      handler.call(ProtobufUtil.getMutateCall(commandId.incrementAndGet(), mutateRequest), resultFuture, channel);
      Response result = resultFuture.get(C5Constants.TIMEOUT, TimeUnit.MILLISECONDS);
      if (result.getMutate().getProcessed()) {
        return true;
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new IOException(e.toString());
    }
    return false;
  }


  private Result doGetResult(Get get) throws RuntimeException {
    try {
      return doGetResult(get, false);
    } catch (IOException | InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  private Boolean doExists(Get get) throws RuntimeException {
    try {
      Result getResult = doGetResult(get, true);
      return getResult.getExists();
    } catch (IOException | InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }


  private Result doGetResult(Get get, boolean existsOnly)
      throws RuntimeException, IOException, InterruptedException, ExecutionException, TimeoutException {
    final SettableFuture<Response> resultFuture = SettableFuture.create();
    final GetRequest getRequest;
    getRequest = RequestConverter.buildGetRequest(getRegionName(), get, existsOnly);
    handler.call(ProtobufUtil.getGetCall(commandId.incrementAndGet(), getRequest), resultFuture, channel);
    GetResponse getResponse = resultFuture.get(C5Constants.TIMEOUT, TimeUnit.MILLISECONDS).getGet();
    if (existsOnly) {
      return ProtobufUtil.toResultExists(getResponse.getResult());
    } else {
      return ProtobufUtil.toResult(getResponse.getResult());
    }
  }

  private void doMutation(Mutation mutation, MutationProto.MutationType type) throws RuntimeException {
    final SettableFuture<Response> resultFuture = SettableFuture.create();

    final MutateRequest mutateRequest = RequestConverter.buildMutateRequest(getRegionName(),
        type,
        mutation);
    handler.call(ProtobufUtil.getMutateCall(commandId.incrementAndGet(), mutateRequest), resultFuture, channel);
    try {
      resultFuture.get(C5Constants.TIMEOUT, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | TimeoutException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private void doPut(Mutation mutation) throws RuntimeException {
    doMutation(mutation, MutationProto.MutationType.PUT);
  }

  private void doDelete(Mutation mutation) throws RuntimeException {
    doMutation(mutation, MutationProto.MutationType.DELETE);
  }


  private byte[] getRegionName() {
    return tableName;
  }
}
