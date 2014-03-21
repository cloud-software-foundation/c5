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

package c5db.client;

import c5db.client.generated.Action;
import c5db.client.generated.Condition;
import c5db.client.generated.GetRequest;
import c5db.client.generated.MutateRequest;
import c5db.client.generated.MutationProto;
import c5db.client.generated.RegionAction;
import c5db.client.generated.RegionSpecifier;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

final public class RequestConverter {

  private RequestConverter() {
    throw new UnsupportedOperationException();
  }

  /**
   * Create a protocol buffer GetRequest for a client Get.
   *
   * @param regionName    the name of the region to get
   * @param get           the client Get
   * @param existenceOnly indicate if check row existence only
   * @return a protocol buffer GetRequest
   */
  public static GetRequest buildGetRequest(final byte[] regionName,
                                           final Get get,
                                           final boolean existenceOnly) throws IOException {
    final RegionSpecifier region = buildRegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME, regionName);
    return new GetRequest(region, ProtobufUtil.toGet(get, existenceOnly));
  }

  /**
   * Convert a byte array to a protocol buffer RegionSpecifier.
   *
   * @param type  the region specifier type
   * @param value the region specifier byte array value
   * @return a protocol buffer RegionSpecifier
   */
  public static RegionSpecifier buildRegionSpecifier(final RegionSpecifier.RegionSpecifierType type,
                                                     final byte[] value) {
    return new RegionSpecifier(type, ByteBuffer.wrap(value));
  }

  /**
   * Create a protocol buffer MutateRequest for a put.
   *
   * @param regionName The region name to request from
   * @param delete     The delete to process
   * @return a mutate request
   */
  public static MutateRequest buildMutateRequest(final byte[] regionName, final Delete delete) {
    RegionSpecifier region = buildRegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME, regionName);
    return new MutateRequest(region,
        ProtobufUtil.toMutation(MutationProto.MutationType.DELETE, delete),
        new Condition());
  }

  /**
   * Create a protocol buffer MutateRequest for a put.
   *
   * @param regionName The region name to create the mutateRequest against
   * @param put        The client request.
   * @return a mutate request
   */
  public static MutateRequest buildMutateRequest(final byte[] regionName, final Put put) {
    RegionSpecifier region = buildRegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME, regionName);
    return new MutateRequest(region, ProtobufUtil.toMutation(MutationProto.MutationType.PUT, put), new Condition());
  }


  /**
   * Create a protocol buffer MultiRequest for row mutations.
   * Does not propagate Action absolute position.
   *
   * @param regionName   The region name the actions apply to.
   * @param rowMutations The row mutations to apply to the region
   * @param atomic       Whether or not the actions should be atomic
   * @return a data-laden RegionAction
   */
  public static RegionAction buildRegionAction(final byte[] regionName,
                                               final boolean atomic,
                                               final RowMutations rowMutations)
      throws IOException {
    final RegionSpecifier region = buildRegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME, regionName);
    final List<Action> actions = new ArrayList<>();
    int index = 0;
    for (Mutation mutation : rowMutations.getMutations()) {
      MutationProto.MutationType mutateType;
      if (mutation instanceof Put) {
        mutateType = MutationProto.MutationType.PUT;
      } else if (mutation instanceof Delete) {
        mutateType = MutationProto.MutationType.DELETE;
      } else {
        throw new DoNotRetryIOException("RowMutations supports only put and delete, not "
            + mutation.getClass().getName());
      }
      final MutationProto mp = ProtobufUtil.toMutation(mutateType, mutation);
      final Action action = new Action(++index, mp, new c5db.client.generated.Get());
      actions.add(action);

    }
    return new RegionAction(region, atomic, actions);
  }
}
