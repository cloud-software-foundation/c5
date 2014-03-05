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

package c5db;


import c5db.client.generated.Column;
import c5db.client.generated.MutationProto;
import c5db.client.generated.NameBytesPair;
import com.dyuproject.protostuff.ByteString;
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

    return CellUtil.createCell(c.getRow().toByteArray(),
        c.getFamily().toByteArray(),
        c.getQualifier().toByteArray(),
        c.getTimestamp(),
        (byte) c.getCellType().getNumber(),
        c.getValue().toByteArray());
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
    c5db.client.generated.Get get_ = new c5db.client.generated.Get();
    get_.setRow(ByteString.copyFrom(get.getRow()));
    get_.setCacheBlocks(get.getCacheBlocks());
    get_.setMaxVersions(get.getMaxVersions());
    if (get.getFilter() != null) {
      get_.setFilter(ProtobufUtil.toFilter(get.getFilter()));
    }
    TimeRange timeRange = get.getTimeRange();
    if (!timeRange.isAllTime()) {
      c5db.client.generated.TimeRange timeRange_ = new c5db.client.generated.TimeRange();
      timeRange_.setFrom(timeRange.getMin());
      timeRange_.setTo(timeRange.getMax());
      get_.setTimeRange(timeRange_);
    }
    Map<String, byte[]> attributes = get.getAttributesMap();
    if (!attributes.isEmpty()) {
      NameBytesPair updatedAttribute = new NameBytesPair();
      for (Map.Entry<String, byte[]> attribute : attributes.entrySet()) {
        updatedAttribute.setName(attribute.getKey());
        updatedAttribute.setValue(ByteString.copyFrom(attribute.getValue()));
        addAttribute(get_, updatedAttribute);
      }
    }
    if (get.hasFamilies()) {

      Map<byte[], NavigableSet<byte[]>> families = get.getFamilyMap();
      for (Map.Entry<byte[], NavigableSet<byte[]>> family : families.entrySet()) {
        Column column = new Column();
        NavigableSet<byte[]> qualifiers = family.getValue();
        column.setFamily(ByteString.copyFrom(family.getKey()));

        if (qualifiers != null && qualifiers.size() > 0) {
          for (byte[] qualifier : qualifiers) {
            addQualifier(column, qualifier);
          }
        }
        addColumn(get_, column);
      }

    }
    if (get.getMaxResultsPerColumnFamily() >= 0) {
      get_.setStoreLimit(get.getMaxResultsPerColumnFamily());
    }
    if (get.getRowOffsetPerColumnFamily() > 0) {
      get_.setStoreOffset(get.getRowOffsetPerColumnFamily());
    }
    get_.setExistenceOnly(existenceOnly);
    return get_;
  }

  private static void addColumn(c5db.client.generated.Get get, Column column) {
    List<Column> columnList = get.getColumnList();
    if (columnList == null){
      columnList = new ArrayList<>();
    }
    columnList.add(column);
    get.setColumnList(columnList);
  }

  private static void addQualifier(Column column, byte[] qualifier) {
    ByteString byteStringQualifier = ByteString.copyFrom(qualifier);
    List<ByteString> qualifierList = column.getQualifierList();
    if (qualifierList == null){
      qualifierList = new ArrayList<>();
    }
    qualifierList.add(byteStringQualifier);
    column.setQualifierList(qualifierList);
  }

  private static void addAttribute(c5db.client.generated.Get get_, NameBytesPair attribute) {
    List<NameBytesPair> attributeList = get_.getAttributeList();
    if (attributeList == null){
      attributeList = new ArrayList<>();
    }

      attributeList.add(attribute);
    get_.setAttributeList(attributeList);
  }

  /**
   * Convert a client Filter to a protocol buffer Filter
   *
   * @param filter_ the Filter to convert
   * @return the converted protocol buffer Filter
   */
  public static c5db.client.generated.Filter toFilter(Filter filter_) throws IOException {
    c5db.client.generated.Filter filter = new c5db.client.generated.Filter();
    filter.setName(filter_.getClass().getName());
    filter.setSerializedFilter(ByteString.copyFrom(filter_.toByteArray()));
    return filter;
  }

  /**
   * Create a protocol buffer Mutate based on a client Mutation
   *
   * @param type     The type of mutation to create
   * @param mutation The client Mutation
   * @return a protobuf'd Mutation
   * @throws IOException
   */
  public static MutationProto toMutation(final MutationProto.MutationType type,
                                         final Mutation mutation)
      throws IOException {
    MutationProto mutationProto = getMutationAndSetCommonFields(type, mutation);

    MutationProto.ColumnValue.QualifierValue qualifierValue = new MutationProto.ColumnValue.QualifierValue();
    for (Map.Entry<byte[], List<KeyValue>> family : mutation.getFamilyMap().entrySet()) {
      MutationProto.ColumnValue columnValue = new MutationProto.ColumnValue();
      columnValue.setFamily(ByteString.copyFrom(family.getKey()));

      for (Cell cell : family.getValue()) {
        KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
        qualifierValue.setQualifier(ByteString.copyFrom(kv.getQualifier()));
        qualifierValue.setValue(ByteString.copyFrom(kv.getValue()));
        qualifierValue.setTimestamp(kv.getTimestamp());
        if (type == MutationProto.MutationType.DELETE) {
          KeyValue.Type keyValueType = KeyValue.Type.codeToType(kv.getType());
          qualifierValue.setDeleteType(toDeleteType(keyValueType));
        }
        addQualifierValue(columnValue, qualifierValue);

      }
      addColumnValue(mutationProto, columnValue);
    }
    return mutationProto;
  }

  private static void addColumnValue(final MutationProto mutationProto, final MutationProto.ColumnValue columnValue) {
    List<MutationProto.ColumnValue> columnValueList = mutationProto.getColumnValueList();
    if (columnValueList == null){
      columnValueList = new ArrayList<>();
    }
    columnValueList.add(columnValue);
    mutationProto.setColumnValueList(columnValueList);
  }

  private static void addQualifierValue(final MutationProto.ColumnValue columnValue,
                                        final MutationProto.ColumnValue.QualifierValue qualifierValue) {
    List<MutationProto.ColumnValue.QualifierValue> qualifierValueList = columnValue.getQualifierValueList();
    if (qualifierValueList == null){
      qualifierValueList = new ArrayList<>();
    }
    qualifierValueList.add(qualifierValue);
    columnValue.setQualifierValueList(qualifierValueList);
  }

  /**
   * Code shared by {@link #toMutation(MutationProto.MutationType, Mutation)} and
   *
   * @param type     The type of mutation to create
   * @param mutation The mutation to get
   * @return A partly-filled out protobuf'd Mutation.
   */
  private static MutationProto getMutationAndSetCommonFields(final MutationProto.MutationType type,
                                                             final Mutation mutation) {
    MutationProto mutationProto = new MutationProto();
    mutationProto.setRow(ByteString.copyFrom(mutation.getRow()));
    mutationProto.setMutateType(type);
    mutationProto.setTimestamp(mutation.getTimeStamp());
    Map<String, byte[]> attributes = mutation.getAttributesMap();
    if (!attributes.isEmpty()) {
      NameBytesPair attributePair = new NameBytesPair();
      for (Map.Entry<String, byte[]> attribute : attributes.entrySet()) {
        attributePair.setName(attribute.getKey());
        attributePair.setValue(ByteString.copyFrom(attribute.getValue()));
        List<NameBytesPair> attributeList = mutationProto.getAttributeList();
        attributeList.add(attributePair);
        mutationProto.setAttributeList(attributeList);
      }
    }
    return mutationProto;
  }

  /**
   * Convert a delete KeyValue type to protocol buffer DeleteType.
   *
   * @param type The delete type to make
   * @return protocol buffer DeleteType
   * @throws IOException
   */
  public static MutationProto.DeleteType toDeleteType(KeyValue.Type type) throws IOException {
    switch (type) {
      case Delete:
        return MutationProto.DeleteType.DELETE_ONE_VERSION;
      case DeleteColumn:
        return MutationProto.DeleteType.DELETE_MULTIPLE_VERSIONS;
      case DeleteFamily:
        return MutationProto.DeleteType.DELETE_FAMILY;
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
  public static c5db.client.generated.Scan toScan(final Scan scan) throws IOException {
    c5db.client.generated.Scan scan_ = new c5db.client.generated.Scan();
    scan_.setCacheBlocks(scan.getCacheBlocks());
    if (scan.getBatch() > 0) {
      scan_.setBatchSize(scan.getBatch());
    }
    if (scan.getMaxResultSize() > 0) {
      scan_.setMaxResultSize(scan.getMaxResultSize());
    }
    Boolean loadColumnFamiliesOnDemand = scan.getLoadColumnFamiliesOnDemandValue();
    if (loadColumnFamiliesOnDemand != null) {
      scan_.setLoadColumnFamiliesOnDemand(loadColumnFamiliesOnDemand);
    }
    scan_.setMaxVersions(scan.getMaxVersions());
    TimeRange timeRange = scan.getTimeRange();
    if (!timeRange.isAllTime()) {
      c5db.client.generated.TimeRange newTimeRange = new c5db.client.generated.TimeRange();
      newTimeRange.setFrom(timeRange.getMin());
      newTimeRange.setTo(timeRange.getMax());
      scan_.setTimeRange(newTimeRange);
    }
    Map<String, byte[]> attributes = scan.getAttributesMap();
    if (!attributes.isEmpty()) {

      for (Map.Entry<String, byte[]> attribute : attributes.entrySet()) {
        NameBytesPair nameBytesPair = new NameBytesPair();
        nameBytesPair.setName(attribute.getKey());
        nameBytesPair.setValue(ByteString.copyFrom(attribute.getValue()));
        addAttribute(scan_, nameBytesPair);
      }
    }
    byte[] startRow = scan.getStartRow();
    if (startRow != null && startRow.length > 0) {
      scan_.setStartRow(ByteString.copyFrom(startRow));
    }
    byte[] stopRow = scan.getStopRow();
    if (stopRow != null && stopRow.length > 0) {
      scan_.setStopRow(ByteString.copyFrom(stopRow));
    }
    if (scan.hasFilter()) {
      scan_.setFilter(ProtobufUtil.toFilter(scan.getFilter()));
    }
    if (scan.hasFamilies()) {
      for (Map.Entry<byte[], NavigableSet<byte[]>>
          family : scan.getFamilyMap().entrySet()) {
        Column column = new Column();
        column.setFamily(ByteString.copyFrom(family.getKey()));
        NavigableSet<byte[]> qualifiers = family.getValue();
        if (qualifiers != null && qualifiers.size() > 0) {
          for (byte[] qualifier : qualifiers) {
            addQualifier(column, qualifier);
          }
        }
        addColumn(scan_, column);
      }
    }
    if (scan.getMaxResultsPerColumnFamily() >= 0) {
      scan_.setStoreLimit(scan.getMaxResultsPerColumnFamily());
    }
    if (scan.getRowOffsetPerColumnFamily() > 0) {
      scan_.setStoreOffset(scan.getRowOffsetPerColumnFamily());
    }
    return scan_;
  }

  private static void addColumn(c5db.client.generated.Scan scan_, Column column) {
    List<Column> columnList = scan_.getColumnList();
    if (columnList == null){
      columnList = new ArrayList<>();
    }
    columnList.add(column);
    scan_.setColumnList(columnList);
  }

  private static void addAttribute(c5db.client.generated.Scan scan_, NameBytesPair nameBytesPair) {
    List<NameBytesPair> attributeList = scan_.getAttributeList();
    if (attributeList == null){
      attributeList = new ArrayList<>();
    }
    attributeList.add(nameBytesPair);
    scan_.setAttributeList(attributeList);
  }
}

