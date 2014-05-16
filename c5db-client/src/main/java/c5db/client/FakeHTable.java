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
package c5db.client;

import c5db.client.generated.ByteArrayComparable;
import c5db.client.generated.Comparator;
import c5db.client.generated.CompareType;
import c5db.client.generated.Condition;
import c5db.client.generated.GetRequest;
import c5db.client.generated.MultiRequest;
import c5db.client.generated.MutateRequest;
import c5db.client.generated.MutationProto;
import c5db.client.generated.RegionAction;
import c5db.client.generated.RegionSpecifier;
import c5db.client.generated.ScanRequest;
import c5db.client.scanner.ClientScannerManager;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import io.protostuff.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;


/**
 * The main client entry point for putting data into C5. Equivalent to HTablet from HBase.
 */
public class FakeHTable implements HTableInterface, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(C5AsyncDatabase.class);
  /**
   * C5Table is the main entry points for clients of C5DB
   *
   * @param tableName The name of the table to connect to.
   */
  private final ClientScannerManager clientScannerManager = ClientScannerManager.INSTANCE;
  byte[] regionName;
  RegionSpecifier regionSpecifier;
  TableInterface c5AsyncDatabase;
  private byte[] tableName;


  public FakeHTable(String hostname, int port, ByteString tableName)
      throws IOException, InterruptedException, TimeoutException, ExecutionException {
    c5AsyncDatabase = new C5AsyncDatabase(hostname, port);
    this.tableName = tableName.toByteArray();
    regionName = tableName.toByteArray();
    regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME, ByteBuffer.wrap(regionName));
  }

  FakeHTable(TableInterface c5AsyncDatabase, ByteString tableName)
      throws IOException, InterruptedException, TimeoutException, ExecutionException {
    this.c5AsyncDatabase = c5AsyncDatabase;
    this.tableName = tableName.toByteArray();
    regionName = tableName.toByteArray();
    regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME, ByteBuffer.wrap(regionName));
  }

  public static Comparator toComparator(ByteArrayComparable comparator) {
    return new Comparator(comparator.getClass().getName(), comparator.getValue());
  }

  @Override
  public Result get(final Get get) throws IOException {
    GetRequest getRequest = RequestConverter.buildGetRequest(regionName, get, false);
    try {
      return ProtobufUtil.toResult(c5AsyncDatabase.dotGetCall(getRequest).get().getGet().getResult());
    } catch (InterruptedException | ExecutionException e) {
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
    GetRequest getRequest = RequestConverter.buildGetRequest(regionName, get, true);
    try {
      return c5AsyncDatabase.dotGetCall(getRequest).get().getGet().getResult().getExists();
    } catch (InterruptedException | ExecutionException e) {
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
    final ScanRequest scanRequest = new ScanRequest(regionSpecifier,
        ProtobufUtil.toScan(scan),
        0L,
        C5Constants.DEFAULT_INIT_SCAN,
        false,
        0L);

    try {
      return clientScannerManager.get(c5AsyncDatabase.doScanCall(scanRequest).get().getScan().getScannerId());
    } catch (Exception e) {
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
  public Boolean[] exists(List<Get> gets) throws IOException {
    //TODO Batch get
    final List<Boolean> results = new ArrayList<>();
    for (Get get : gets) {
      results.add(this.exists(get));
    }
    return results.toArray(new Boolean[results.size()]);
  }

  @Override
  public void put(Put put) throws IOException {
    MutateRequest mutateRequest = RequestConverter.buildMutateRequest(regionName, MutationProto.MutationType.PUT, put);
    try {
      if (!c5AsyncDatabase.doMutateCall(mutateRequest).get().getMutate().getProcessed()) {
        throw new IOException("Not processed");
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void put(List<Put> puts) throws IOException {
    for (Put put : puts) {
      this.put(put);
    }
  }

  @Override
  public void delete(Delete delete) throws IOException {
    MutateRequest mutateRequest = RequestConverter.buildMutateRequest(regionName,
        MutationProto.MutationType.DELETE,
        delete);
    try {
      if (!c5AsyncDatabase.doMutateCall(mutateRequest).get().getMutate().getProcessed()) {
        throw new IOException("Not processed");
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void delete(List<Delete> deletes) throws IOException {
    for (Delete delete : deletes) {
      this.delete(delete);
    }
  }

  @Override
  public void mutateRow(RowMutations rm) throws IOException {
    RegionAction regionAction = RequestConverter.buildRegionAction(regionName, true, rm);
    List<RegionAction> regionActions = Arrays.asList(regionAction);
    try {
      c5AsyncDatabase.doMultiCall(new MultiRequest(regionActions)).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException {

    Condition condition = new Condition(ByteBuffer.wrap(row),
        ByteBuffer.wrap(family),
        ByteBuffer.wrap(qualifier),
        CompareType.EQUAL,
        toComparator(new ByteArrayComparable(ByteBuffer.wrap(value))));
    MutateRequest mutateRequest = RequestConverter.buildMutateRequest(regionName,
        MutationProto.MutationType.PUT,
        put,
        condition);
    try {
      if (!c5AsyncDatabase.doMutateCall(mutateRequest).get().getMutate().getProcessed()) {
        return false;
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
    return true;
  }

  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete)
      throws IOException {
    Condition condition = new Condition(ByteBuffer.wrap(row),
        ByteBuffer.wrap(family),
        ByteBuffer.wrap(qualifier),
        CompareType.EQUAL,
        toComparator(new ByteArrayComparable(ByteBuffer.wrap(value))));
    MutateRequest mutateRequest = RequestConverter.buildMutateRequest(regionName,
        MutationProto.MutationType.DELETE,
        delete,
        condition);
    try {
      if (!c5AsyncDatabase.doMutateCall(mutateRequest).get().getMutate().getProcessed()) {
        return false;
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
    return true;
  }


  @Override
  public void close() {
    try {
      c5AsyncDatabase.close();
    } catch (Exception e) {
      LOG.error("Error closing:" + e);
    }
  }


  @Override
  public Result append(Append append) throws IOException {
    return null;
  }

  @Override
  public Result increment(Increment increment) throws IOException {
    return null;
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) throws IOException {
    return 0;
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, Durability durability) throws IOException {
    return 0;
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL) throws IOException {
    return 0;
  }


  @Override
  public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
    throw new IOException("We don't support getClosestRow");
  }

  @Override
  public boolean isAutoFlush() {
    LOG.error("we auto flush by default");
    return true;
  }

  @Override
  public void setAutoFlush(boolean autoFlush) {
    LOG.error("Unspported");

  }

  @Override
  public void flushCommits() throws IOException {
    LOG.error("we auto flush by default");
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(byte[] row) {
    LOG.error("Unspported");
    return null;
  }

  @Override
  public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service, byte[] startKey, byte[] endKey, Batch.Call<T, R> callable) throws ServiceException, Throwable {
    LOG.error("Unspported");
    return null;
  }

  @Override
  public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey, byte[] endKey, Batch.Call<T, R> callable, Batch.Callback<R> callback) throws ServiceException, Throwable {
    LOG.error("Unspported");
  }

  @Override
  public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
    LOG.error("Unspported");
  }

  @Override
  public void setAutoFlushTo(boolean autoFlush) {
    LOG.error("Unspported");
  }

  @Override
  public long getWriteBufferSize() {
    LOG.error("Unspported");
    return 0;
  }

  @Override
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    LOG.error("Unspported");
  }

  @Override
  public byte[] getTableName() {
    return this.tableName;
  }

  @Override
  public TableName getName() {
    return null;
  }

  public Configuration getConfiguration() {
    return null;
  }

  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    return null;
  }

  public void batch(List<? extends Row> actions, Object[] results) {
  }

  public Object[] batch(List<? extends Row> actions) {
    return new Object[0];
  }

  public <R> void batchCallback(List<? extends Row> actions, Object[] results, Batch.Callback<R> callback) {
  }

  public <R> Object[] batchCallback(List<? extends Row> actions, Batch.Callback<R> callback) {
    return new Object[0];
  }

  public class InvalidResponse extends Exception {
    public InvalidResponse(String s) {
    }
  }

  public class InvalidScannerResults extends Exception {
    public InvalidScannerResults(String s) {
    }
  }

}
