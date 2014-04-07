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

package c5db.regionserver;


import c5db.client.generated.CellType;
import c5db.client.generated.Comparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.DynamicClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * A set of static functions to map between the HBase API and the protocolbuffers API.
 */
public class ReverseProtobufUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ReverseProtobufUtil.class);

  /**
   * Dynamic class loader to load filter/comparators
   */
  private final static ClassLoader CLASS_LOADER;

  static {
    ClassLoader parent = ReverseProtobufUtil.class.getClassLoader();
    Configuration conf = HBaseConfiguration.create();
    CLASS_LOADER = new DynamicClassLoader(conf, parent);
  }

  /**
   * Convert a client Result to a protocol buffer Result
   *
   * @param result the client Result to convert
   * @return the converted protocol buffer Result
   */
  public static c5db.client.generated.Result toResult(final Result result) {
    List<c5db.client.generated.Cell> retCells = new ArrayList<>();
    int retCellCount = 0;
    boolean retExists = false;
    Cell[] cells = result.rawCells();
    if (cells != null) {
      for (Cell c : cells) {
        addCell(retCells, toCell(c));
      }
    }

    if (result.getExists() != null) {
      retExists = result.getExists();
    }

    return new c5db.client.generated.Result(retCells, retCellCount, retExists);
  }

  private static void addCell(List<c5db.client.generated.Cell> cellList, c5db.client.generated.Cell cell) {
    if (cellList == null) {
      cellList = new ArrayList<>();
    }
    cellList.add(cell);
  }

  public static c5db.client.generated.Cell toCell(final Cell kv) {
    ByteBuffer row = ByteBuffer.wrap(kv.getFamilyArray(), kv.getRowOffset(), kv.getRowLength());
    ByteBuffer family = ByteBuffer.wrap(kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength());
    ByteBuffer qualifier = ByteBuffer.wrap(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength());
    long timestamp = kv.getTimestamp();
    CellType cellType = CellType.valueOf(kv.getTypeByte());
    ByteBuffer value = ByteBuffer.wrap(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength());

    return new c5db.client.generated.Cell(row, family, qualifier, timestamp, cellType, value);
  }

  /**
   * Convert a protocol buffer Get to a client Get
   *
   * @param proto the protocol buffer Get to convert
   * @return the converted client Get
   * @throws IOException
   */
  public static Get toGet(final c5db.client.generated.Get proto) throws IOException {
    if (proto == null) {
      return null;
    }
    byte[] row = proto.getRow().array();
    Get get = new Get(row);
    get.setCacheBlocks(proto.getCacheBlocks());
    // TODO we probably need a better way of managing these.
    if (proto.getMaxVersions() != 0) {
      get.setMaxVersions(proto.getMaxVersions());
    }
    if (proto.getStoreLimit() != 0) {
      get.setMaxResultsPerColumnFamily(proto.getStoreLimit());
    }
    if (proto.getStoreOffset() != 0) {
      get.setRowOffsetPerColumnFamily(proto.getStoreOffset());
    }
    if (proto.getTimeRange() != null) {
      c5db.client.generated.TimeRange timeRange = proto.getTimeRange();

      long minStamp = timeRange.getFrom();
      long maxStamp = timeRange.getTo();
      // TODO REMOVE
      LOG.trace("minStamp: " + minStamp);
      LOG.trace("maxStamp: " + minStamp);

      get.setTimeRange(minStamp, maxStamp);
    }
    if (proto.getFilter() != null) {
      c5db.client.generated.Filter filter = proto.getFilter();
      get.setFilter(toFilter(filter));
    }
    for (c5db.client.generated.NameBytesPair attribute : proto.getAttributeList()) {
      get.setAttribute(attribute.getName(), attribute.getValue().array());
    }
    if (proto.getColumnList() != null && proto.getColumnList().size() > 0) {
      for (c5db.client.generated.Column column : proto.getColumnList()) {
        byte[] family = column.getFamily().array();

        for (ByteBuffer qualifier : column.getQualifierList()) {
          get.addColumn(family, qualifier.array());
        }
        get.addFamily(family);

      }
    }
    if (proto.getExistenceOnly()) {
      get.setCheckExistenceOnly(true);
    }
    if (proto.getClosestRowBefore()) {
      get.setClosestRowBefore(true);
    }
    return get;
  }

  /**
   * Convert a protocol buffer Filter to a client Filter
   *
   * @param proto the protocol buffer Filter to convert
   * @return the converted Filter
   */
  private static Filter toFilter(c5db.client.generated.Filter proto) throws IOException {
    String type = proto.getName();
    final byte[] value = proto.getSerializedFilter().array();
    String funcName = "parseFrom";
    try {
      Class<? extends Filter> c =
          (Class<? extends Filter>) Class.forName(type, true, CLASS_LOADER);
      Method parseFrom = c.getMethod(funcName, byte[].class);
      if (parseFrom == null) {
        throw new IOException("Unable to locate function: " + funcName + " in type: " + type);
      }
      return (Filter) parseFrom.invoke(c, value);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Convert a protocol buffer Scan to a client Scan
   *
   * @param proto the protocol buffer Scan to convert
   * @return the converted client Scan
   * @throws IOException
   */
  public static Scan toScan(
      final c5db.client.generated.Scan proto) throws IOException {
    byte[] startRow = HConstants.EMPTY_START_ROW;
    byte[] stopRow = HConstants.EMPTY_END_ROW;
    if (proto.getStartRow() != null) {
      startRow = proto.getStartRow().array();
    }
    if (proto.getStopRow() != null) {
      stopRow = proto.getStopRow().array();
    }
    Scan scan = new Scan(startRow, stopRow);
    scan.setCacheBlocks(proto.getCacheBlocks());
    scan.setMaxVersions(proto.getMaxVersions());
    scan.setMaxResultsPerColumnFamily(proto.getStoreLimit());
    scan.setRowOffsetPerColumnFamily(proto.getStoreOffset());
    scan.setLoadColumnFamiliesOnDemand(proto.getLoadColumnFamiliesOnDemand());
    if (proto.getTimeRange() != null) {
      c5db.client.generated.TimeRange timeRange = proto.getTimeRange();
      long minStamp = timeRange.getFrom();
      long maxStamp = timeRange.getTo();
      // TODO REMOVE
      System.out.println("minStamp: " + minStamp);
      System.out.println("maxStamp: " + minStamp);
      scan.setTimeRange(minStamp, maxStamp);
    }
    if (proto.getFilter() != null && proto.getFilter().getName() != null) {
      c5db.client.generated.Filter filter = proto.getFilter();
      scan.setFilter(toFilter(filter));
    }
    scan.setBatch(proto.getBatchSize());
    scan.setMaxResultSize(proto.getMaxResultSize());
    scan.setSmall(proto.getSmall());
    for (c5db.client.generated.NameBytesPair attribute : proto.getAttributeList()) {
      scan.setAttribute(attribute.getName(), attribute.getValue().array());
    }

    for (c5db.client.generated.Column column : proto.getColumnList()) {
      byte[] family = column.getFamily().array();
      for (ByteBuffer qualifier : column.getQualifierList()) {
        scan.addColumn(family, qualifier.array());
      }
      scan.addFamily(family);
    }
    return scan;
  }

  /**
   * Convert a protocol buffer Mutate to a Put.
   *
   * @param proto The protocol buffer MutationProto to convert
   * @return A client Put.
   * @throws IOException
   */
  public static Put toPut(final c5db.client.generated.MutationProto proto) throws IOException {
    // TODO: Server-side at least why do we convert back to the Client types?  Why not just pb it?
    c5db.client.generated.MutationProto.MutationType type = proto.getMutateType();
    assert type == c5db.client.generated.MutationProto.MutationType.PUT : type.name();
    byte[] row = proto.getRow() != null ? proto.getRow().array() : null;
    long timestamp = proto.getTimestamp();
    Put put = new Put(row, timestamp);
    // The proto has the metadata and the data itself
    for (c5db.client.generated.MutationProto.ColumnValue column : proto.getColumnValueList()) {
      byte[] family = column.getFamily().array();
      for (c5db.client.generated.MutationProto.ColumnValue.QualifierValue qv : column.getQualifierValueList()) {
        byte[] qualifier = qv.getQualifier().array();
        if (qv.getValue() == null) {
          throw new DoNotRetryIOException("Missing required field: qualifier value");
        }
        byte[] value = qv.getValue().array();
        long ts = qv.getTimestamp();
        put.add(family, qualifier, ts, value);
      }
    }


    put.setDurability(toDurability(proto.getDurability()));
    for (c5db.client.generated.NameBytesPair attribute : proto.getAttributeList()) {
      put.setAttribute(attribute.getName(), attribute.getValue().array());
    }
    return put;
  }

  /**
   * Convert a protocol buffer Mutate to a Delete
   *
   * @param proto the protocol buffer Mutate to convert
   * @return the converted client Delete
   */
  public static Delete toDelete(final c5db.client.generated.MutationProto proto) {
    c5db.client.generated.MutationProto.MutationType type = proto.getMutateType();
    assert type == c5db.client.generated.MutationProto.MutationType.DELETE : type.name();
    byte[] row = proto.getRow() != null ? proto.getRow().array() : null;
    long timestamp = proto.getTimestamp();

    Delete delete = new Delete(row, timestamp);
    for (c5db.client.generated.MutationProto.ColumnValue column : proto.getColumnValueList()) {
      byte[] family = column.getFamily().array();
      for (c5db.client.generated.MutationProto.ColumnValue.QualifierValue qv : column.getQualifierValueList()) {
        c5db.client.generated.MutationProto.DeleteType deleteType = qv.getDeleteType();
        byte[] qualifier = null;
        if (qv.getQualifier() != null) {
          qualifier = qv.getQualifier().array();
        }
        long ts = qv.getTimestamp();

        if (deleteType == c5db.client.generated.MutationProto.DeleteType.DELETE_ONE_VERSION) {
          delete.deleteColumn(family, qualifier, ts);
        } else if (deleteType == c5db.client.generated.MutationProto.DeleteType.DELETE_MULTIPLE_VERSIONS) {
          delete.deleteColumns(family, qualifier, ts);
        } else if (deleteType == c5db.client.generated.MutationProto.DeleteType.DELETE_FAMILY_VERSION) {
          delete.deleteFamilyVersion(family, ts);
        } else {
          delete.deleteFamily(family, ts);
        }
      }
    }

    delete.setDurability(toDurability(proto.getDurability()));
    for (c5db.client.generated.NameBytesPair attribute : proto.getAttributeList()) {
      delete.setAttribute(attribute.getName(), attribute.getValue().array());
    }
    return delete;
  }

  private static Durability toDurability(c5db.client.generated.MutationProto.Durability proto) {
    switch (proto) {
      case USE_DEFAULT:
        return Durability.USE_DEFAULT;
      case SKIP_WAL:
        return Durability.SKIP_WAL;
      case ASYNC_WAL:
        return Durability.ASYNC_WAL;
      case SYNC_WAL:
        return Durability.SYNC_WAL;
      case FSYNC_WAL:
        return Durability.FSYNC_WAL;
      default:
        return Durability.USE_DEFAULT;
    }
  }

  /**
   * Print out some subset of a MutationProto rather than all of it and its data
   *
   * @param proto Protobuf to print out
   * @return Short String of mutation proto
   */
  static String toShortString(final c5db.client.generated.MutationProto proto) {
    return "row=" + Bytes.toString(proto.getRow().array()) +
        ", type=" + proto.getMutateType().toString();
  }

  //TODO support more than byte comparable
  public static ByteArrayComparable toComparator(Comparator comparator) {
    byte[] serializedArray = comparator.getSerializedComparator().array();
    c5db.client.generated.ByteArrayComparable byteArrayComparable =
        new c5db.client.generated.ByteArrayComparable(ByteBuffer.wrap(serializedArray));
    c5db.client.generated.BinaryComparator binaryComparator
        = new c5db.client.generated.BinaryComparator(byteArrayComparable);
    return new BinaryComparator(binaryComparator.getComparable().getValue().array());
  }
}
