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
package c5db.client.scanner;

import c5db.client.ProtobufUtil;
import c5db.client.TableInterface;
import c5db.client.generated.LocationResponse;
import c5db.client.generated.RegionLocation;
import c5db.client.generated.RegionSpecifier;
import c5db.client.generated.Scan;
import c5db.client.generated.ScanRequest;
import c5db.client.generated.TableName;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;

/**
 * Mostly an HBase-ism. Handles scanner logic for htable users.
 */
public class ClientScanner extends AbstractClientScanner {
  private static final Logger LOG = LoggerFactory.getLogger(ClientScanner.class);
  private final ScanRequest scanRequest;
  private final TableInterface c5AsyncDatabase;
  private final TableName tableName;
  private C5ClientScanner c5ClientScanner;
  ConcurrentLinkedDeque<LocationResponse> nextLocations = new ConcurrentLinkedDeque<>();

  public ClientScanner(TableInterface c5AsyncDatabase, TableName tableName, ScanRequest scanRequest)
      throws ExecutionException, InterruptedException {
    this.c5AsyncDatabase = c5AsyncDatabase;
    this.tableName = tableName;
    ListenableFuture<C5ClientScanner> scanResultFuture = c5AsyncDatabase.scan(tableName, scanRequest);
    this.c5ClientScanner = scanResultFuture.get();
    this.scanRequest = scanRequest;
  }

  @Override
  public Result next() throws IOException {
    try {
      c5db.client.generated.Result result = c5ClientScanner.next();

      while ((result == null && (c5ClientScanner.getNextLocations().size() > 0 || nextLocations.size() > 0))) {
        while (!c5ClientScanner.getNextLocations().isEmpty()) {
          nextLocations.add(c5ClientScanner.getNextLocations().remove());
        }
        c5ClientScanner = getNewClientScanner(nextLocations.remove(), scanRequest);
        result = c5ClientScanner.next();
      }

      return ProtobufUtil.toResult(result);
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
  }

  private ScanRequest prepareNextScan(final ScanRequest scanRequest, ByteBuffer startKeyForNextScan) {
    Scan oldScan = scanRequest.getScan();
    Scan newScan = new Scan(oldScan.getColumnList(),
        oldScan.getAttributeList(),
        startKeyForNextScan,
        oldScan.getStopRow(),
        oldScan.getFilter(),
        oldScan.getTimeRange(),
        oldScan.getMaxVersions(),
        oldScan.getCacheBlocks(),
        oldScan.getBatchSize(),
        oldScan.getMaxResultSize(),
        oldScan.getStoreLimit(),
        oldScan.getStoreOffset(),
        oldScan.getLoadColumnFamiliesOnDemand(),
        oldScan.getSmall());
    return new ScanRequest(scanRequest.getRegion(),
        newScan,
        0l, // restart the scan
        scanRequest.getNumberOfRows(),
        scanRequest.getCloseScanner(),
        scanRequest.getNextCallSeq());
  }

  private C5ClientScanner getNewClientScanner(LocationResponse nextLocation,
                                              ScanRequest scanRequest)
      throws ExecutionException, InterruptedException {

    ByteBuffer startRow = getStartKeyForNextScan(nextLocation.getRegionLocationList().iterator().next());
    scanRequest = prepareNextScan(scanRequest, startRow);
    LOG.warn("Getting the next client scanner for startRow:"
        + Bytes.toString(scanRequest.getScan().getStartRow().array()));

    ScanRequest newScanRequest = new ScanRequest(new RegionSpecifier(),
        scanRequest.getScan(),
        0l,
        scanRequest.getNumberOfRows(),
        scanRequest.getCloseScanner(),
        scanRequest.getNextCallSeq());

    ListenableFuture<C5ClientScanner> scanResultFuture = c5AsyncDatabase.scan(tableName, newScanRequest);
    return scanResultFuture.get();
  }

  public ByteBuffer getStartKeyForNextScan(RegionLocation next) {
    return next.getStartKey();
  }

  @Override
  public Result[] next(int nbRows) throws IOException {

    try {
      c5db.client.generated.Result[] protoResults = c5ClientScanner.next(nbRows);
      return Arrays.asList(protoResults).stream().map(ProtobufUtil::toResult).toArray(Result[]::new);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() {
    try {
      c5ClientScanner.close();
    } catch (InterruptedException e) {
      LOG.error("Unable to close without an error");
      e.printStackTrace();
    }
  }
}
