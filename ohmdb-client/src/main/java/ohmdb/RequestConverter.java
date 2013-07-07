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
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.exceptions.DoNotRetryIOException;

import java.io.IOException;
import java.util.Collection;

public class RequestConverter {


  /**
   * Create a protocol buffer GetRequest for a client Get
   *
   * @param regionName    the name of the region to get
   * @param get           the client Get
   * @param existenceOnly indicate if check row existence only
   * @return a protocol buffer GetRequest
   */
  public static ClientProtos.GetRequest buildGetRequest(final byte[] regionName,
                                                        final Get get, final boolean existenceOnly) throws IOException {
    ClientProtos.GetRequest.Builder builder = ClientProtos.GetRequest.newBuilder();
    HBaseProtos.RegionSpecifier region = buildRegionSpecifier(
        HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME, regionName);
    builder.setExistenceOnly(existenceOnly);
    builder.setRegion(region);
    builder.setGet(ProtobufUtil.toGet(get));
    return builder.build();
  }

  /**
   * Convert a byte array to a protocol buffer RegionSpecifier
   *
   * @param type  the region specifier type
   * @param value the region specifier byte array value
   * @return a protocol buffer RegionSpecifier
   */
  public static HBaseProtos.RegionSpecifier buildRegionSpecifier(
      final HBaseProtos.RegionSpecifier.RegionSpecifierType type, final byte[] value) {
    HBaseProtos.RegionSpecifier.Builder regionBuilder = HBaseProtos.RegionSpecifier.newBuilder();
    regionBuilder.setValue(ByteString.copyFrom(value));
    regionBuilder.setType(type);
    return regionBuilder.build();
  }

  /**
   * Create a protocol buffer MutateRequest for a put
   *
   * @param regionName The region name to request from
   * @param delete     The delete to process
   * @return a mutate request
   * @throws IOException
   */
  public static ClientProtos.MutateRequest buildMutateRequest(
      final byte[] regionName, final Delete delete) throws IOException {
    ClientProtos.MutateRequest.Builder builder = ClientProtos.MutateRequest.newBuilder();
    HBaseProtos.RegionSpecifier region = buildRegionSpecifier(
        HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    builder.setMutation(ProtobufUtil.toMutation(ClientProtos.MutationProto.MutationType.DELETE, delete));
    return builder.build();
  }


  /**
   * Create a protocol buffer MutateRequest for a put
   *
   * @param regionName
   * @param put
   * @return a mutate request
   * @throws IOException
   */
  public static ClientProtos.MutateRequest buildMutateRequest(
      final byte[] regionName, final Put put) throws IOException {
    ClientProtos.MutateRequest.Builder builder = ClientProtos.MutateRequest.newBuilder();
    HBaseProtos.RegionSpecifier region = buildRegionSpecifier(
        HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    builder.setMutation(ProtobufUtil.toMutation(ClientProtos.MutationProto.MutationType.PUT, put));
    return builder.build();
  }

  /**
   * Create a protocol buffer MultiGetRequest for client Gets All gets are going to be run against
   * the same region.
   *
   * @param regionName    the name of the region to get from
   * @param gets          the client Gets
   * @param existenceOnly indicate if check rows existence only
   * @return a protocol buffer MultiGetRequest
   */
  public static ClientProtos.MultiGetRequest buildMultiGetRequest(final byte[] regionName,
                                                                  final Collection<Get> gets,
                                                                  final boolean existenceOnly) throws IOException {
    ClientProtos.MultiGetRequest.Builder builder = ClientProtos.MultiGetRequest.newBuilder();
    HBaseProtos.RegionSpecifier region =
        buildRegionSpecifier(HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME, regionName);
    builder.setExistenceOnly(existenceOnly);
    builder.setRegion(region);
    for (Get get : gets) {
      builder.addGet(ProtobufUtil.toGet(get));
    }
    return builder.build();
  }

  /**
   * Create a protocol buffer MultiRequest for a row mutations
   *
   * @param regionName
   * @param rowMutations
   * @return a multi request
   * @throws IOException
   */
  public static ClientProtos.MultiRequest buildMultiRequest(final byte[] regionName,
                                                            final RowMutations rowMutations)
      throws IOException {
    ClientProtos.MultiRequest.Builder builder = getMultiRequestBuilderWithRegionAndAtomicSet(regionName, true);
    for (Mutation mutation : rowMutations.getMutations()) {
      ClientProtos.MutationProto.MutationType mutateType;
      if (mutation instanceof Put) {
        mutateType = ClientProtos.MutationProto.MutationType.PUT;
      } else if (mutation instanceof Delete) {
        mutateType = ClientProtos.MutationProto.MutationType.DELETE;
      } else {
        throw new DoNotRetryIOException("RowMutations supports only put and delete, not " +
            mutation.getClass().getName());
      }
      ClientProtos.MutationProto mp = ProtobufUtil.toMutation(mutateType, mutation);
      builder.addAction(ClientProtos.MultiAction.newBuilder().setMutation(mp).build());
    }
    return builder.build();
  }


  private static ClientProtos.MultiRequest.Builder
  getMultiRequestBuilderWithRegionAndAtomicSet(final byte[] regionName,
                                               final boolean atomic) {
    ClientProtos.MultiRequest.Builder builder = ClientProtos.MultiRequest.newBuilder();
    HBaseProtos.RegionSpecifier region =
        buildRegionSpecifier(HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME,
            regionName);
    builder.setRegion(region);
    return builder.setAtomic(atomic);
  }

}
