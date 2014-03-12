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

/** Incorporates changes licensed under:
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package c5db;


import c5db.client.generated.Call;
import c5db.client.generated.Column;
import c5db.client.generated.GetRequest;
import c5db.client.generated.MultiRequest;
import c5db.client.generated.MutateRequest;
import c5db.client.generated.MutationProto;
import c5db.client.generated.NameBytesPair;
import c5db.client.generated.ScanRequest;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

public class ProtobufUtil {
  private ProtobufUtil() {
  }

  /**
   * Convert a protocol buffer Result to a client Result
   *
   * @param proto the protocol buffer Result to convert
   * @return the converted client Result
   */
  public static Result toResult(final c5db.client.generated.Result proto) {
    if (proto == null) {
      return null;
    }
    List<c5db.client.generated.Cell> values = proto.getCellList();
    List<Cell> cells = new ArrayList<>();
    for (c5db.client.generated.Cell c : values) {
      cells.add(toCell(c));
    }
    return Result.create(cells);
  }

  private static Cell toCell(c5db.client.generated.Cell c) {

    return CellUtil.createCell(c.getRow().array(),
        c.getFamily().array(),
        c.getQualifier().array(),
        c.getTimestamp(),
        (byte) c.getCellType().getNumber(),
        c.getValue().array());
  }

  /**
   * Create a protocol buffer Get based on a client Get.
   *
   * @param get           The client Get.
   * @param existenceOnly Is this only an existence check.
   * @return a protocol buffer Get
   * @throws IOException
   */
  public static c5db.client.generated.Get toGet(final Get get, boolean existenceOnly) throws IOException {

    c5db.client.generated.TimeRange timeRange;

    ByteBuffer row = ByteBuffer.wrap(get.getRow());
    boolean cacheBlocks = get.getCacheBlocks();
    int maxVersions = get.getMaxVersions();
    List<Column> columns = new ArrayList<>();
    List<NameBytesPair> attributes = new ArrayList<>();

    int storeLimit;
    int storeOffset;

    c5db.client.generated.Filter filter = get.getFilter() == null ? null : ProtobufUtil.toFilter(get.getFilter());

    if (!get.getTimeRange().isAllTime()) {
      timeRange = new c5db.client.generated.TimeRange(get.getTimeRange().getMin(), get.getTimeRange().getMax());
    } else {
      timeRange = new c5db.client.generated.TimeRange(0, Long.MAX_VALUE);
    }
    Map<String, byte[]> attributesMap = get.getAttributesMap();
    if (!attributes.isEmpty()) {
      for (Map.Entry<String, byte[]> attribute : attributesMap.entrySet()) {
        NameBytesPair updatedAttribute = new NameBytesPair(attribute.getKey(), ByteBuffer.wrap(attribute.getValue()));
        attributes.add(updatedAttribute);
      }
    }
    if (get.hasFamilies()) {
      Map<byte[], NavigableSet<byte[]>> families = get.getFamilyMap();
      for (Map.Entry<byte[], NavigableSet<byte[]>> family : families.entrySet()) {
        List<ByteBuffer> qualifiers = new ArrayList<>();
        for (byte[] qualifier : family.getValue()) {
          qualifiers.add(ByteBuffer.wrap(qualifier));
        }

        Column column = new Column(ByteBuffer.wrap(family.getKey()), qualifiers);
        columns.add(column);
      }


    }
    storeLimit = get.getMaxResultsPerColumnFamily();
    storeOffset = get.getRowOffsetPerColumnFamily();

    return new c5db.client.generated.Get(row,
        columns,
        attributes,
        filter,
        timeRange,
        maxVersions, cacheBlocks,
        storeLimit,
        storeOffset,
        existenceOnly,
        false);
  }

  /**
   * Convert a client Filter to a protocol buffer Filter
   *
   * @param filter the Filter to convert
   * @return the converted protocol buffer Filter
   */
  public static c5db.client.generated.Filter toFilter(Filter filter) throws IOException {
    if (filter == null){
      return new c5db.client.generated.Filter();
    }
    return new c5db.client.generated.Filter(filter.getClass().getName(), ByteBuffer.wrap(filter.toByteArray()));
  }

  /**
   * Create a protocol buffer Mutate based on a client Mutation
   *
   * @param type     The type of mutation to create
   * @param mutation The client Mutation
   * @return a protobuf'd Mutation
   */
  public static MutationProto toMutation(final MutationProto.MutationType type,
                                         final Mutation mutation) {

    List<MutationProto.ColumnValue> columnValues = new ArrayList<>();
    List<NameBytesPair> attributes = new ArrayList<>();

    final MutableInt cellCount = new MutableInt(0);
    mutation.getFamilyCellMap().forEach((family, cells) -> {
      List<MutationProto.ColumnValue.QualifierValue> qualifierValues = new ArrayList<>();
      for (Cell cell : cells) {
        KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
        cellCount.add(1);
        ByteBuffer qualifier = ByteBuffer.wrap(cell.getQualifierArray(),
            cell.getQualifierOffset(),
            cell.getQualifierLength());

        ByteBuffer value = ByteBuffer.wrap(cell.getValueArray(),
            cell.getValueOffset(),
            cell.getValueLength());

        MutationProto.DeleteType deleteType;
        if (type == MutationProto.MutationType.DELETE) {
          KeyValue.Type keyValueType = KeyValue.Type.codeToType(kv.getTypeByte());
          deleteType = toDeleteType(keyValueType);
        } else {
          deleteType = null;
        }

        MutationProto.ColumnValue.QualifierValue qualifierValue =
            new MutationProto.ColumnValue.QualifierValue(qualifier, value, cell.getTimestamp(), deleteType);
        qualifierValues.add(qualifierValue);
      }
      columnValues.add(new MutationProto.ColumnValue(ByteBuffer.wrap(family), qualifierValues));

    });


    MutationProto.Durability durability = MutationProto.Durability.valueOf(mutation.getDurability().ordinal());

    return new MutationProto(ByteBuffer.wrap(mutation.getRow()),
        type,
        columnValues,
        mutation.getTimeStamp(),
        attributes,
        durability,
        new c5db.client.generated.TimeRange(),
        cellCount.intValue());

  }


  /**
   * Convert a delete KeyValue type to protocol buffer DeleteType.
   *
   * @param type The delete type to make
   * @return protocol buffer DeleteType
   */
  public static MutationProto.DeleteType toDeleteType(KeyValue.Type type) {
    switch (type) {
      case Delete:
        return MutationProto.DeleteType.DELETE_ONE_VERSION;
      case DeleteColumn:
        return MutationProto.DeleteType.DELETE_MULTIPLE_VERSIONS;
      case DeleteFamily:
        return MutationProto.DeleteType.DELETE_FAMILY;
      default:
        return null;
    }
  }

  /**
   * Convert a client Scan to a protocol buffer Scan
   *
   * @param scan the client Scan to convert
   * @return the converted protocol buffer Scan
   * @throws IOException
   */
  public static c5db.client.generated.Scan toScan(final Scan scan) throws IOException {

    boolean cacheBlocks = scan.getCacheBlocks();
    int batchSize = scan.getBatch();
    long maxResultSize = scan.getMaxResultSize();

    Boolean loadColumnFamiliesOnDemand = scan.getLoadColumnFamiliesOnDemandValue();
    if (loadColumnFamiliesOnDemand == null){
      loadColumnFamiliesOnDemand = false;
    }
    int maxVersions = scan.getMaxVersions();
    c5db.client.generated.TimeRange timeRange;
    if (scan.getTimeRange().isAllTime()) {
      timeRange = new c5db.client.generated.TimeRange(0, Long.MAX_VALUE);
    } else {
      timeRange = new c5db.client.generated.TimeRange(scan.getTimeRange().getMin(), scan.getTimeRange().getMax());
    }


    List<NameBytesPair> attributes = new ArrayList<>();
    Map<String, byte[]> attributesMap = scan.getAttributesMap();
    if (!attributes.isEmpty()) {
      for (Map.Entry<String, byte[]> attribute : attributesMap.entrySet()) {
        attributes.add(new NameBytesPair(attribute.getKey(), ByteBuffer.wrap(attribute.getValue())));
      }
    }


    ByteBuffer startRow = ByteBuffer.wrap(scan.getStartRow());
    ByteBuffer stopRow = ByteBuffer.wrap(scan.getStopRow());
    c5db.client.generated.Filter filter = ProtobufUtil.toFilter(scan.getFilter());

    List<Column> columns = new ArrayList<>();
    if (scan.hasFamilies()) {
      for (Map.Entry<byte[], NavigableSet<byte[]>>
          family : scan.getFamilyMap().entrySet()) {
        NavigableSet<byte[]> qualifierSet = family.getValue();
        List<ByteBuffer> qualifiers = new ArrayList<>();
        if (qualifierSet != null && qualifierSet.size() > 0) {
          for (byte[] qualifier : qualifierSet) {
            qualifiers.add(ByteBuffer.wrap(qualifier));
          }
        }
        Column column = new Column(ByteBuffer.wrap(family.getKey()), qualifiers);
        columns.add(column);
      }
    }

    int storeLimit = scan.getMaxResultsPerColumnFamily();
    int storeOffset = scan.getRowOffsetPerColumnFamily();

    return new c5db.client.generated.Scan(columns,
        attributes,
        startRow,
        stopRow,
        filter,
        timeRange,
        maxVersions,
        cacheBlocks,
        batchSize,
        maxResultSize,
        storeLimit,
        storeOffset,
        loadColumnFamiliesOnDemand,
        false);
  }

  public static Call getGetCall(long commandId, GetRequest get) throws IOException {
    return new Call(Call.Command.GET, commandId, get, null,  null, null);
  }

  public static Call getMutateCall(long commandId, MutateRequest mutateRequest) throws IOException {
    return new Call(Call.Command.MUTATE, commandId, null, mutateRequest,  null, null);
  }

  public static Call getScanCall(long commandId, ScanRequest scanRequest) throws IOException {
    return new Call(Call.Command.SCAN, commandId, null, null,  scanRequest, null);
  }

  public static Call getMultiCall(long commandId, MultiRequest multiRequest) throws IOException {
    return new Call(Call.Command.MULTI, commandId, null, null,  null, multiRequest);
  }
}

