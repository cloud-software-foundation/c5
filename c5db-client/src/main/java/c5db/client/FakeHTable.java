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
import io.protostuff.ByteString;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;


/**
 * The main client entry point for putting data into C5. Equivalent to HTablet from HBase.
 */
public class FakeHTable implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(FakeHTable.class);
  private final ClientScannerManager clientScannerManager = ClientScannerManager.INSTANCE;
  private byte[] regionName;
  private RegionSpecifier regionSpecifier;
  private TableInterface c5AsyncDatabase;
  private byte[] tableName;

  /**
   * A mock HTable Client
   *
   * @param tableName The name of the table to connect to.
   */
  public FakeHTable(String hostname, int port, ByteString tableName)
      throws InterruptedException, TimeoutException, ExecutionException {
    c5AsyncDatabase = new C5AsyncDatabase(hostname, port);
    this.tableName = tableName.toByteArray();
    regionName = tableName.toByteArray();
    regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME, ByteBuffer.wrap(regionName));
  }

  FakeHTable(TableInterface c5AsyncDatabase, ByteString tableName) {
    this.c5AsyncDatabase = c5AsyncDatabase;
    this.tableName = tableName.toByteArray();
    regionName = tableName.toByteArray();
    regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME, ByteBuffer.wrap(regionName));
  }

  private static Comparator toComparator(ByteArrayComparable comparator) {
    return new Comparator(comparator.getClass().getName(), comparator.getValue());
  }

  public Result get(final Get get) throws IOException {
    GetRequest getRequest = RequestConverter.buildGetRequest(regionName, get, false);
    try {
      return ProtobufUtil.toResult(c5AsyncDatabase.dotGetCall(getRequest).get().getGet().getResult());
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
  }

  public Result[] get(List<Get> gets) throws IOException {
    //TODO Batch get
    final List<Result> results = new ArrayList<>();
    for (Get get : gets) {
      results.add(this.get(get));
    }
    return results.toArray(new Result[results.size()]);
  }

  public boolean exists(final Get get) throws IOException {
    GetRequest getRequest = RequestConverter.buildGetRequest(regionName, get, true);
    try {
      return c5AsyncDatabase.dotGetCall(getRequest).get().getGet().getResult().getExists();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
  }

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
      return clientScannerManager.get(c5AsyncDatabase.doScanCall(scanRequest).get());
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public ResultScanner getScanner(byte[] family) throws IOException {
    final Scan scan = new Scan();
    scan.addFamily(family);
    return getScanner(scan);
  }

  public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
    final Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    return getScanner(scan);
  }

  public Boolean[] exists(List<Get> gets) throws IOException {
    //TODO Batch get
    final List<Boolean> results = new ArrayList<>();
    for (Get get : gets) {
      results.add(this.exists(get));
    }
    return results.toArray(new Boolean[results.size()]);
  }

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

  public void put(List<Put> puts) throws IOException {
    for (Put put : puts) {
      this.put(put);
    }
  }

  void delete(Delete delete) throws IOException {
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

  public void delete(List<Delete> deletes) throws IOException {
    for (Delete delete : deletes) {
      this.delete(delete);
    }
  }

  public void mutateRow(RowMutations rm) throws IOException {
    RegionAction regionAction = RequestConverter.buildRegionAction(regionName, rm);
    List<RegionAction> regionActions = Arrays.asList(regionAction);
    try {
      c5AsyncDatabase.doMultiCall(new MultiRequest(regionActions)).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
  }

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

  public byte[] getTableName() {
    return this.tableName;
  }

  private class InvalidResponse extends Exception {
    public InvalidResponse(String s) {
    }
  }

  private class InvalidScannerResults extends Exception {
    public InvalidScannerResults(String s) {
    }
  }
}
