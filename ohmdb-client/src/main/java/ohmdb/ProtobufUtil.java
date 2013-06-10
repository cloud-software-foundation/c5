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

/**
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

package ohmdb;

import com.google.protobuf.ByteString;
import ohmdb.client.generated.ClientProtos;
import ohmdb.client.generated.HBaseProtos;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;

import java.io.IOException;
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
  public static Result toResult(final ClientProtos.Result proto) {
    if (proto == null) {
      return null;
    }
    List<HBaseProtos.Cell> values = proto.getCellList();
    List<Cell> cells = new ArrayList<>(values.size());
    for (HBaseProtos.Cell c : values) {
      cells.add(toCell(c));
    }
    return new Result(cells);
  }

  public static Cell toCell(final HBaseProtos.Cell cell) {
    // Doing this is going to kill us if we do it for all data passed.
    // St.Ack 20121205
    return CellUtil.createCell(cell.getRow().toByteArray(),
        cell.getFamily().toByteArray(),
        cell.getQualifier().toByteArray(),
        cell.getTimestamp(),
        (byte) cell.getCellType().getNumber(),
        cell.getValue().toByteArray());
  }

  /**
   * Create a protocol buffer Get based on a client Get.
   *
   * @param get the client Get
   * @return a protocol buffer Get
   * @throws IOException
   */
  public static ClientProtos.Get toGet(final Get get) throws IOException {
    ClientProtos.Get.Builder builder =
        ClientProtos.Get.newBuilder();
    builder.setRow(ByteString.copyFrom(get.getRow()));
    builder.setCacheBlocks(get.getCacheBlocks());
    builder.setMaxVersions(get.getMaxVersions());
    if (get.getFilter() != null) {
      builder.setFilter(ProtobufUtil.toFilter(get.getFilter()));
    }
    TimeRange timeRange = get.getTimeRange();
    if (!timeRange.isAllTime()) {
      HBaseProtos.TimeRange.Builder timeRangeBuilder =
          HBaseProtos.TimeRange.newBuilder();
      timeRangeBuilder.setFrom(timeRange.getMin());
      timeRangeBuilder.setTo(timeRange.getMax());
      builder.setTimeRange(timeRangeBuilder.build());
    }
    Map<String, byte[]> attributes = get.getAttributesMap();
    if (!attributes.isEmpty()) {
      HBaseProtos.NameBytesPair.Builder attributeBuilder = HBaseProtos.NameBytesPair.newBuilder();
      for (Map.Entry<String, byte[]> attribute : attributes.entrySet()) {
        attributeBuilder.setName(attribute.getKey());
        attributeBuilder.setValue(ByteString.copyFrom(attribute.getValue()));
        builder.addAttribute(attributeBuilder.build());
      }
    }
    if (get.hasFamilies()) {
      ClientProtos.Column.Builder columnBuilder = ClientProtos.Column.newBuilder();
      Map<byte[], NavigableSet<byte[]>> families = get.getFamilyMap();
      for (Map.Entry<byte[], NavigableSet<byte[]>> family : families.entrySet()) {
        NavigableSet<byte[]> qualifiers = family.getValue();
        columnBuilder.setFamily(ByteString.copyFrom(family.getKey()));
        columnBuilder.clearQualifier();
        if (qualifiers != null && qualifiers.size() > 0) {
          for (byte[] qualifier : qualifiers) {
            columnBuilder.addQualifier(ByteString.copyFrom(qualifier));
          }
        }
        builder.addColumn(columnBuilder.build());
      }
    }
    if (get.getMaxResultsPerColumnFamily() >= 0) {
      builder.setStoreLimit(get.getMaxResultsPerColumnFamily());
    }
    if (get.getRowOffsetPerColumnFamily() > 0) {
      builder.setStoreOffset(get.getRowOffsetPerColumnFamily());
    }
    return builder.build();
  }

  /**
   * Convert a client Filter to a protocol buffer Filter
   *
   * @param filter the Filter to convert
   * @return the converted protocol buffer Filter
   */
  public static HBaseProtos.Filter toFilter(Filter filter) throws IOException {
    HBaseProtos.Filter.Builder builder = HBaseProtos.Filter.newBuilder();
    builder.setName(filter.getClass().getName());
    builder.setSerializedFilter(ByteString.copyFrom(filter.toByteArray()));
    return builder.build();
  }


  /**
   * Create a protocol buffer Mutate based on a client Mutation
   *
   * @param type
   * @param mutation
   * @return a protobuf'd Mutation
   * @throws IOException
   */
  public static ClientProtos.MutationProto toMutation(final ClientProtos.MutationProto.MutationType type,
                                                      final Mutation mutation)
      throws IOException {
    ClientProtos.MutationProto.Builder builder = getMutationBuilderAndSetCommonFields(type, mutation);
    ClientProtos.MutationProto.ColumnValue.Builder columnBuilder = ClientProtos.MutationProto.ColumnValue.newBuilder();
    ClientProtos.MutationProto.ColumnValue.QualifierValue.Builder valueBuilder = ClientProtos.MutationProto.ColumnValue.QualifierValue.newBuilder();
    for (Map.Entry<byte[], List<? extends Cell>> family : mutation.getFamilyMap().entrySet()) {
      columnBuilder.setFamily(ByteString.copyFrom(family.getKey()));
      columnBuilder.clearQualifierValue();
      for (Cell cell : family.getValue()) {
        KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
        valueBuilder.setQualifier(ByteString.copyFrom(kv.getQualifier()));
        valueBuilder.setValue(ByteString.copyFrom(kv.getValue()));
        valueBuilder.setTimestamp(kv.getTimestamp());
        if (type == ClientProtos.MutationProto.MutationType.DELETE) {
          KeyValue.Type keyValueType = KeyValue.Type.codeToType(kv.getType());
          valueBuilder.setDeleteType(toDeleteType(keyValueType));
        }
        columnBuilder.addQualifierValue(valueBuilder.build());
      }
      builder.addColumnValue(columnBuilder.build());
    }
    return builder.build();
  }

  /**
   * Code shared by {@link #toMutation(ClientProtos.MutationProto.MutationType, Mutation)} and
   * {@link #toMutationNoData(ClientProtos.MutationProto.MutationType, Mutation)}
   *
   * @param type
   * @param mutation
   * @return A partly-filled out protobuf'd Mutation.
   */
  private static ClientProtos.MutationProto.Builder
  getMutationBuilderAndSetCommonFields(final ClientProtos.MutationProto.MutationType type,
                                       final Mutation mutation) {
    ClientProtos.MutationProto.Builder builder = ClientProtos.MutationProto.newBuilder();
    builder.setRow(ByteString.copyFrom(mutation.getRow()));
    builder.setMutateType(type);
    builder.setTimestamp(mutation.getTimeStamp());
    Map<String, byte[]> attributes = mutation.getAttributesMap();
    if (!attributes.isEmpty()) {
      HBaseProtos.NameBytesPair.Builder attributeBuilder = HBaseProtos.NameBytesPair.newBuilder();
      for (Map.Entry<String, byte[]> attribute : attributes.entrySet()) {
        attributeBuilder.setName(attribute.getKey());
        attributeBuilder.setValue(ByteString.copyFrom(attribute.getValue()));
        builder.addAttribute(attributeBuilder.build());
      }
    }
    return builder;
  }

  /**
   * Convert a delete KeyValue type to protocol buffer DeleteType.
   *
   * @param type
   * @return protocol buffer DeleteType
   * @throws IOException
   */
  public static ClientProtos.MutationProto.DeleteType toDeleteType(KeyValue.Type type) throws IOException {
    switch (type) {
      case Delete:
        return ClientProtos.MutationProto.DeleteType.DELETE_ONE_VERSION;
      case DeleteColumn:
        return ClientProtos.MutationProto.DeleteType.DELETE_MULTIPLE_VERSIONS;
      case DeleteFamily:
        return ClientProtos.MutationProto.DeleteType.DELETE_FAMILY;
      default:
        throw new IOException("Unknown delete type: " + type);
    }
  }


  /**
   * Convert a client Scan to a protocol buffer Scan
   *
   * @param scan the client Scan to convert
   * @return the converted protocol buffer Scan
   * @throws IOException
   */
  public static ClientProtos.Scan toScan(final Scan scan) throws IOException {
    ClientProtos.Scan.Builder scanBuilder =
        ClientProtos.Scan.newBuilder();
    scanBuilder.setCacheBlocks(scan.getCacheBlocks());
    if (scan.getBatch() > 0) {
      scanBuilder.setBatchSize(scan.getBatch());
    }
    if (scan.getMaxResultSize() > 0) {
      scanBuilder.setMaxResultSize(scan.getMaxResultSize());
    }
    Boolean loadColumnFamiliesOnDemand = scan.getLoadColumnFamiliesOnDemandValue();
    if (loadColumnFamiliesOnDemand != null) {
      scanBuilder.setLoadColumnFamiliesOnDemand(loadColumnFamiliesOnDemand);
    }
    scanBuilder.setMaxVersions(scan.getMaxVersions());
    TimeRange timeRange = scan.getTimeRange();
    if (!timeRange.isAllTime()) {
      HBaseProtos.TimeRange.Builder timeRangeBuilder =
          HBaseProtos.TimeRange.newBuilder();
      timeRangeBuilder.setFrom(timeRange.getMin());
      timeRangeBuilder.setTo(timeRange.getMax());
      scanBuilder.setTimeRange(timeRangeBuilder.build());
    }
    Map<String, byte[]> attributes = scan.getAttributesMap();
    if (!attributes.isEmpty()) {
      HBaseProtos.NameBytesPair.Builder attributeBuilder = HBaseProtos.NameBytesPair.newBuilder();
      for (Map.Entry<String, byte[]> attribute : attributes.entrySet()) {
        attributeBuilder.setName(attribute.getKey());
        attributeBuilder.setValue(ByteString.copyFrom(attribute.getValue()));
        scanBuilder.addAttribute(attributeBuilder.build());
      }
    }
    byte[] startRow = scan.getStartRow();
    if (startRow != null && startRow.length > 0) {
      scanBuilder.setStartRow(ByteString.copyFrom(startRow));
    }
    byte[] stopRow = scan.getStopRow();
    if (stopRow != null && stopRow.length > 0) {
      scanBuilder.setStopRow(ByteString.copyFrom(stopRow));
    }
    if (scan.hasFilter()) {
      scanBuilder.setFilter(ProtobufUtil.toFilter(scan.getFilter()));
    }
    if (scan.hasFamilies()) {
      ClientProtos.Column.Builder columnBuilder = ClientProtos.Column.newBuilder();
      for (Map.Entry<byte[], NavigableSet<byte[]>>
          family : scan.getFamilyMap().entrySet()) {
        columnBuilder.setFamily(ByteString.copyFrom(family.getKey()));
        NavigableSet<byte[]> qualifiers = family.getValue();
        columnBuilder.clearQualifier();
        if (qualifiers != null && qualifiers.size() > 0) {
          for (byte[] qualifier : qualifiers) {
            columnBuilder.addQualifier(ByteString.copyFrom(qualifier));
          }
        }
        scanBuilder.addColumn(columnBuilder.build());
      }
    }
    if (scan.getMaxResultsPerColumnFamily() >= 0) {
      scanBuilder.setStoreLimit(scan.getMaxResultsPerColumnFamily());
    }
    if (scan.getRowOffsetPerColumnFamily() > 0) {
      scanBuilder.setStoreOffset(scan.getRowOffsetPerColumnFamily());
    }
    return scanBuilder.build();
  }
}

