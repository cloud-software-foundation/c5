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


import com.dyuproject.protostuff.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.DynamicClassLoader;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class ReverseProtobufUtil {

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
    c5db.client.generated.Result builder = new c5db.client.generated.Result();
    Cell[] cells = result.rawCells();
    if (cells != null) {
      for (Cell c : cells) {
        addCell(builder, toCell(c));
      }
    }
    if (result.getExists() != null) {
      builder.setExists(result.getExists());
    }
    return builder;
  }

  private static void addCell(c5db.client.generated.Result result, c5db.client.generated.Cell cell) {
    List<c5db.client.generated.Cell> cellList = result.getCellList();
    if (cellList == null) {
      cellList = new ArrayList<>();
    }
    cellList.add(cell);
    result.setCellList(cellList);
  }

  public static c5db.client.generated.Cell toCell(final Cell kv) {
    // Doing this is going to kill us if we do it for all data passed.
    // St.Ack 20121205
    c5db.client.generated.Cell kvbuilder = new c5db.client.generated.Cell();
    kvbuilder.setRow(ByteString.copyFrom(kv.getRowArray(), kv.getRowOffset(),
        kv.getRowLength()));
    kvbuilder.setFamily(ByteString.copyFrom(kv.getFamilyArray(),
        kv.getFamilyOffset(), kv.getFamilyLength()));
    kvbuilder.setQualifier(ByteString.copyFrom(kv.getQualifierArray(),
        kv.getQualifierOffset(), kv.getQualifierLength()));
    kvbuilder.setCellType(c5db.client.generated.CellType.valueOf(kv.getTypeByte()));
    kvbuilder.setTimestamp(kv.getTimestamp());
    kvbuilder.setValue(ByteString.copyFrom(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength()));
    return kvbuilder;
  }

  /**
   * Convert a protocol buffer Filter to a client Filter
   *
   * @param proto the protocol buffer Filter to convert
   * @return the converted Filter

   @SuppressWarnings("unchecked")
   public static Filter toFilter(c5db.client.generated.Filter proto) throws IOException {
   String type = proto.getName();
   final byte[] value = proto.getSerializedFilter().toByteArray();
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
   }*/

  /**
   * Convert a protocol buffer Get to a client Get
   *
   * @param proto the protocol buffer Get to convert
   * @return the converted client Get
   * @throws IOException
   */
  public static Get toGet(final c5db.client.generated.Get proto) throws IOException {
    if (proto == null) return null;
    byte[] row = proto.getRow().toByteArray();
    Get get = new Get(row);
    if (proto.getCacheBlocks() != null) {
      get.setCacheBlocks(proto.getCacheBlocks());
    }
    if (proto.getMaxVersions() != null) {
      get.setMaxVersions(proto.getMaxVersions());
    }
    if (proto.getStoreLimit() != null) {
      get.setMaxResultsPerColumnFamily(proto.getStoreLimit());
    }
    if (proto.getStoreOffset() != null) {
      get.setRowOffsetPerColumnFamily(proto.getStoreOffset());
    }
    if (proto.getTimeRange() != null) {
      c5db.client.generated.TimeRange timeRange = proto.getTimeRange();
      long minStamp = 0;
      long maxStamp = Long.MAX_VALUE;
      if (timeRange.getFrom() != null) {
        minStamp = timeRange.getFrom();
      }
      if (timeRange.getTo() != null) {
        maxStamp = timeRange.getTo();
      }
      get.setTimeRange(minStamp, maxStamp);
    }
    if (proto.getFilter() != null) {
      c5db.client.generated.Filter filter = proto.getFilter();
      get.setFilter(toFilter(filter));
    }
    for (c5db.client.generated.NameBytesPair attribute : proto.getAttributeList()) {
      get.setAttribute(attribute.getName(), attribute.getValue().toByteArray());
    }
    if (proto.getColumnList() != null && proto.getColumnList().size() > 0) {
      for (c5db.client.generated.Column column : proto.getColumnList()) {
        byte[] family = column.getFamily().toByteArray();

        for (ByteString qualifier : column.getQualifierList()) {
          get.addColumn(family, qualifier.toByteArray());
        }
        get.addFamily(family);

      }
    }
    if (proto.getExistenceOnly() != null && proto.getExistenceOnly()) {
      get.setCheckExistenceOnly(true);
    }
    if (proto.getClosestRowBefore() != null && proto.getClosestRowBefore()) {
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
  public static Filter toFilter(c5db.client.generated.Filter proto) throws IOException {
    String type = proto.getName();
    final byte[] value = proto.getSerializedFilter().toByteArray();
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
      startRow = proto.getStartRow().toByteArray();
    }
    if (proto.getStopRow() != null) {
      stopRow = proto.getStopRow().toByteArray();
    }
    Scan scan = new Scan(startRow, stopRow);
    if (proto.getCacheBlocks() != null) {
      scan.setCacheBlocks(proto.getCacheBlocks());
    }
    if (proto.getMaxVersions() != null) {
      scan.setMaxVersions(proto.getMaxVersions());
    }
    if (proto.getStoreLimit() != null) {
      scan.setMaxResultsPerColumnFamily(proto.getStoreLimit());
    }
    if (proto.getStoreOffset() != null) {
      scan.setRowOffsetPerColumnFamily(proto.getStoreOffset());
    }
    if (proto.getLoadColumnFamiliesOnDemand() != null) {
      scan.setLoadColumnFamiliesOnDemand(proto.getLoadColumnFamiliesOnDemand());
    }
    if (proto.getTimeRange() != null) {
      c5db.client.generated.TimeRange timeRange = proto.getTimeRange();
      long minStamp = 0;
      long maxStamp = Long.MAX_VALUE;
      if (timeRange.getFrom() != null) {
        minStamp = timeRange.getFrom();
      }
      if (timeRange.getTo() != null) {
        maxStamp = timeRange.getTo();
      }
      scan.setTimeRange(minStamp, maxStamp);
    }
    if (proto.getFilter() != null) {
      c5db.client.generated.Filter filter = proto.getFilter();
      scan.setFilter(toFilter(filter));
    }
    if (proto.getBatchSize() != null) {
      scan.setBatch(proto.getBatchSize());
    }
    if (proto.getMaxResultSize() != null) {
      scan.setMaxResultSize(proto.getMaxResultSize());
    }
    if (proto.getSmall() != null) {
      scan.setSmall(proto.getSmall());
    }
    for (c5db.client.generated.NameBytesPair attribute : proto.getAttributeList()) {
      scan.setAttribute(attribute.getName(), attribute.getValue().toByteArray());
    }

    for (c5db.client.generated.Column column : proto.getColumnList()) {
      byte[] family = column.getFamily().toByteArray();
      for (ByteString qualifier : column.getQualifierList()) {
        scan.addColumn(family, qualifier.toByteArray());
      }
      scan.addFamily(family);
    }
    return scan;
  }

  /**
   * Convert a protocol buffer Mutate to a Put.
   *
   * @param proto       The protocol buffer MutationProto to convert
   * @param cellScanner If non-null, the Cell data that goes with this proto.
   * @return A client Put.
   * @throws IOException
   */
  public static Put toPut(final c5db.client.generated.MutationProto proto, final CellScanner cellScanner)
      throws IOException {
    // TODO: Server-side at least why do we convert back to the Client types?  Why not just pb it?
    c5db.client.generated.MutationProto.MutationType type = proto.getMutateType();
    assert type == c5db.client.generated.MutationProto.MutationType.PUT : type.name();
    byte[] row = proto.getRow() != null ? proto.getRow().toByteArray() : null;
    long timestamp = proto.getTimestamp() != null ? proto.getTimestamp() : HConstants.LATEST_TIMESTAMP;
    Put put = null;
    int cellCount = proto.getAssociatedCellCount() != null ? proto.getAssociatedCellCount() : 0;
    if (cellCount > 0) {
      // The proto has metadata only and the data is separate to be found in the cellScanner.
      if (cellScanner == null) {
        throw new DoNotRetryIOException("Cell count of " + cellCount + " but no cellScanner: " +
            toShortString(proto));
      }
      for (int i = 0; i < cellCount; i++) {
        if (!cellScanner.advance()) {
          throw new DoNotRetryIOException("Cell count of " + cellCount + " but at index " + i +
              " no cell returned: " + toShortString(proto));
        }
        Cell cell = cellScanner.current();
        if (put == null) {
          put = new Put(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), timestamp);
        }
        put.add(KeyValueUtil.ensureKeyValue(cell));
      }
    } else {
      put = new Put(row, timestamp);
      // The proto has the metadata and the data itself
      for (c5db.client.generated.MutationProto.ColumnValue column : proto.getColumnValueList()) {
        byte[] family = column.getFamily().toByteArray();
        for (c5db.client.generated.MutationProto.ColumnValue.QualifierValue qv : column.getQualifierValueList()) {
          byte[] qualifier = qv.getQualifier().toByteArray();
          if (qv.getValue() == null) {
            throw new DoNotRetryIOException(
                "Missing required field: qualifer value");
          }
          byte[] value = qv.getValue().toByteArray();
          long ts = timestamp;
          if (qv.getTimestamp() != null) {
            ts = qv.getTimestamp();
          }
          put.add(family, qualifier, ts, value);
        }
      }
    }
    put.setDurability(toDurability(proto.getDurability()));
    for (c5db.client.generated.NameBytesPair attribute : proto.getAttributeList()) {
      put.setAttribute(attribute.getName(), attribute.getValue().toByteArray());
    }
    return put;
  }

  /**
   * Convert a protocol buffer Mutate to a Delete
   *
   * @param proto       the protocol buffer Mutate to convert
   * @param cellScanner if non-null, the data that goes with this delete.
   * @return the converted client Delete
   * @throws IOException
   */
  public static Delete toDelete(final c5db.client.generated.MutationProto proto, final CellScanner cellScanner)
      throws IOException {
    c5db.client.generated.MutationProto.MutationType type = proto.getMutateType();
    assert type == c5db.client.generated.MutationProto.MutationType.DELETE : type.name();
    byte[] row = proto.getRow() != null ? proto.getRow().toByteArray() : null;
    long timestamp = HConstants.LATEST_TIMESTAMP;
    if (proto.getTimestamp() != null) {
      timestamp = proto.getTimestamp();
    }
    Delete delete = null;
    int cellCount = proto.getAssociatedCellCount() != null ? proto.getAssociatedCellCount() : 0;
    if (cellCount > 0) {
      // The proto has metadata only and the data is separate to be found in the cellScanner.
      if (cellScanner == null) {
        // TextFormat should be fine for a Delete since it carries no data, just coordinates.
        throw new DoNotRetryIOException("Cell count of " + cellCount + " but no cellScanner: " +
            proto);
      }
      for (int i = 0; i < cellCount; i++) {
        if (!cellScanner.advance()) {
          // TextFormat should be fine for a Delete since it carries no data, just coordinates.
          throw new DoNotRetryIOException("Cell count of " + cellCount + " but at index " + i +
              " no cell returned: " + proto);
        }
        Cell cell = cellScanner.current();
        if (delete == null) {
          delete =
              new Delete(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), timestamp);
        }
        delete.addDeleteMarker(KeyValueUtil.ensureKeyValue(cell));
      }
    } else {
      delete = new Delete(row, timestamp);
      for (c5db.client.generated.MutationProto.ColumnValue column : proto.getColumnValueList()) {
        byte[] family = column.getFamily().toByteArray();
        for (c5db.client.generated.MutationProto.ColumnValue.QualifierValue qv : column.getQualifierValueList()) {
          c5db.client.generated.MutationProto.DeleteType deleteType = qv.getDeleteType();
          byte[] qualifier = null;
          if (qv.getQualifier() != null) {
            qualifier = qv.getQualifier().toByteArray();
          }
          long ts = HConstants.LATEST_TIMESTAMP;
          if (qv.getTimestamp() != null) {
            ts = qv.getTimestamp();
          }
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
    }
    delete.setDurability(toDurability(proto.getDurability()));
    for (c5db.client.generated.NameBytesPair attribute : proto.getAttributeList()) {
      delete.setAttribute(attribute.getName(), attribute.getValue().toByteArray());
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
    return "row=" + Bytes.toString(proto.getRow().toByteArray()) +
        ", type=" + proto.getMutateType().toString();
  }
}
