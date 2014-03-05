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

import c5db.client.generated.Action;
import c5db.client.generated.GetRequest;
import c5db.client.generated.MutateRequest;
import c5db.client.generated.MutationProto;
import c5db.client.generated.RegionAction;
import c5db.client.generated.RegionSpecifier;
import com.dyuproject.protostuff.ByteString;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;

import java.io.IOException;
import java.util.List;

public class RequestConverter {


  /**
   * Create a protocol buffer GetRequest for a client Get
   *
   * @param regionName    the name of the region to get
   * @param get           the client Get
   * @param existenceOnly indicate if check row existence only
   * @return a protocol buffer GetRequest
   */
  public static GetRequest buildGetRequest(final byte[] regionName,
                                           final Get get,
                                           final boolean existenceOnly) throws IOException {
    GetRequest getRequest = new GetRequest();
    RegionSpecifier region = buildRegionSpecifier(
        RegionSpecifier.RegionSpecifierType.REGION_NAME, regionName);

    getRequest.setRegion(region);
    getRequest.setGet(ProtobufUtil.toGet(get, existenceOnly));
    return getRequest;
  }

  /**
   * Convert a byte array to a protocol buffer RegionSpecifier
   *
   * @param type  the region specifier type
   * @param value the region specifier byte array value
   * @return a protocol buffer RegionSpecifier
   */
  public static RegionSpecifier buildRegionSpecifier(
      final RegionSpecifier.RegionSpecifierType type, final byte[] value) {
    RegionSpecifier regionSpecifier = new RegionSpecifier();
    regionSpecifier.setValue(ByteString.copyFrom(value));
    regionSpecifier.setType(type);
    return regionSpecifier;
  }

  /**
   * Create a protocol buffer MutateRequest for a put
   *
   * @param regionName The region name to request from
   * @param delete     The delete to process
   * @return a mutate request
   * @throws IOException
   */
  public static MutateRequest buildMutateRequest(final byte[] regionName, final Delete delete) throws IOException {
    MutateRequest mutateRequest = new MutateRequest();
    RegionSpecifier region = buildRegionSpecifier(
        RegionSpecifier.RegionSpecifierType.REGION_NAME, regionName);
    mutateRequest.setRegion(region);
    mutateRequest.setMutation(ProtobufUtil.toMutation(MutationProto.MutationType.DELETE, delete));
    return mutateRequest;
  }


  /**
   * Create a protocol buffer MutateRequest for a put
   *
   * @param regionName The region name to create the mutateRequest against
   * @param put        The client request.
   * @return a mutate request
   * @throws IOException
   */
  public static MutateRequest buildMutateRequest(final byte[] regionName, final Put put) throws IOException {
    MutateRequest mutationRequest = new MutateRequest();
    RegionSpecifier region = buildRegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME, regionName);
    mutationRequest.setRegion(region);
    mutationRequest.setMutation(ProtobufUtil.toMutation(MutationProto.MutationType.PUT, put));
    return mutationRequest;
  }


  private static RegionAction getRegionActionBuilderWithRegion(final byte[] regionName) {
    RegionAction regionAction = new RegionAction();
    RegionSpecifier region = buildRegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME, regionName);
    regionAction.setRegion(region);
    return regionAction;
  }

  /**
   * Create a protocol buffer MultiRequest for row mutations.
   * Does not propagate Action absolute position.  Does not set atomic action on the created
   * RegionAtomic.  Caller should do that if wanted.
   *
   * @param regionName   The region name the actions apply to.
   * @param rowMutations The row mutations to apply to the region
   * @return a data-laden RegionAction
   */
  public static RegionAction buildRegionAction(final byte[] regionName,
                                               final RowMutations rowMutations)
      throws IOException {
    RegionAction regionAction = getRegionActionBuilderWithRegion(regionName);
    for (Mutation mutation : rowMutations.getMutations()) {
      MutationProto.MutationType mutateType;
      if (mutation instanceof Put) {
        mutateType = MutationProto.MutationType.PUT;
      } else if (mutation instanceof Delete) {
        mutateType = MutationProto.MutationType.DELETE;
      } else {
        throw new DoNotRetryIOException("RowMutations supports only put and delete, not " +
            mutation.getClass().getName());
      }
      MutationProto mp = ProtobufUtil.toMutation(mutateType, mutation);
      Action action = new Action();
      action.setMutation(mp);
      addAction(action, regionAction);
    }
    return regionAction;
  }

  private static void addAction(Action action, RegionAction regionAction) {
    List<Action> actionList = regionAction.getActionList();
    actionList.add(action);
    regionAction.setActionList(actionList);
  }

}
