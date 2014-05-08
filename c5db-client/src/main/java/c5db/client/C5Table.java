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
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.Channel;
import io.protostuff.ByteString;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  private final String hostname;

  private static final Logger LOG = LoggerFactory.getLogger(C5Table.class);
  private final Map<String, HRegionInfo> scannerCache = new HashMap<>();

  public C5Table(ByteString tableName) throws IOException, InterruptedException, TimeoutException, ExecutionException {
    this("localhost", tableName, C5Constants.TEST_PORT);
  }

  /**
   * C5Table is the main entry points for clients of C5DB
   *
   * @param tableName The name of the table to connect to.
   */
  public C5Table(ByteString tableName, int port)
      throws IOException, InterruptedException, TimeoutException, ExecutionException {
    this("localhost", tableName, port);
  }

  /**
   * C5Table is the main entry points for clients of C5DB
   *
   * @param tableName The name of the table to connect to.
   */
  public C5Table(String hostname, ByteString tableName, int port)
      throws IOException, InterruptedException, TimeoutException, ExecutionException {
    super(tableName);

    // TODO Route data so we don't need to connect to meta
    this.hostname = hostname;
    this.port = port;

    ByteString metaTableName = ByteString.copyFrom(Bytes.toBytes("hbase:meta"));
    if (!tableName.equals(metaTableName)){
      C5Table metaScan = new C5Table(hostname, metaTableName, port);
      Scan scan = new Scan(tableName.toByteArray());
      scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
      ResultScanner scanner = metaScan.getScanner(scan);

      Result result;
      do {
        result = scanner.next();
        if (result == null){
          return;
        }
        LOG.info("Found meta entry:" + result);
        try {
          HRegionInfo hregionInfo = HRegionInfo.parseFrom(result.getValue(HConstants.CATALOG_FAMILY,
              HConstants.REGIONINFO_QUALIFIER));
          this.scannerCache.put(result.toString(), hregionInfo);
        } catch (DeserializationException e) {
          System.exit(1);
          e.printStackTrace();
        }

      } while(result != null);
    } else {
      LOG.error("They actually connected to meta now what do we do?");
    }

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
      Channel channel = c5ConnectionManager.getOrCreateChannel(this.hostname, this.port);
      MessageHandler handler = channel.pipeline().get(MessageHandler.class);
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
      Channel channel = c5ConnectionManager.getOrCreateChannel(this.hostname, this.port);
      MessageHandler handler = channel.pipeline().get(MessageHandler.class);
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
        ByteBuffer.wrap(getTableName()));

    final ScanRequest scanRequest = new ScanRequest(regionSpecifier,
        ProtobufUtil.toScan(scan),
        0L,
        C5Constants.DEFAULT_INIT_SCAN,
        false,
        0L);

    try {
      Channel channel = c5ConnectionManager.getOrCreateChannel(this.hostname, this.port);
      MessageHandler handler = channel.pipeline().get(MessageHandler.class);
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
    final MutateRequest mutateRequest = RequestConverter.buildMutateRequest(getRegionName(),
        MutationProto.MutationType.PUT,
        put);

    try {
      Channel channel = c5ConnectionManager.getOrCreateChannel(this.hostname, this.port);
      MessageHandler handler = channel.pipeline().get(MessageHandler.class);
      handler.call(ProtobufUtil.getMutateCall(commandId.incrementAndGet(), mutateRequest), resultFuture, channel);
      resultFuture.get(C5Constants.TIMEOUT, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException | IOException e) {
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
    final MutateRequest mutateRequest = RequestConverter.buildMutateRequest(getRegionName(),
        MutationProto.MutationType.DELETE,
        delete);

    try {
      Channel channel = c5ConnectionManager.getOrCreateChannel(this.hostname, this.port);
      MessageHandler handler = channel.pipeline().get(MessageHandler.class);
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
      Channel channel = c5ConnectionManager.getOrCreateChannel(this.hostname, this.port);
      MessageHandler handler = channel.pipeline().get(MessageHandler.class);
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
    try {
      c5ConnectionManager.close();
    } catch (InterruptedException e) {
      System.exit(1);
      e.printStackTrace();
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
      Channel channel = c5ConnectionManager.getOrCreateChannel(this.hostname, this.port);
      MessageHandler handler = channel.pipeline().get(MessageHandler.class);
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
      Channel channel = c5ConnectionManager.getOrCreateChannel(this.hostname, this.port);
      MessageHandler handler = channel.pipeline().get(MessageHandler.class);
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


}
