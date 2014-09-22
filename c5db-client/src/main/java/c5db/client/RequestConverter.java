/*
 * Copyright 2014 WANdisco
 *
 *  WANdisco licenses this file to you under the Apache License,
 *  version 2.0 (the "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations
 *  under the License.
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

/**
 * A utility class responsible for generating requests to c5.
 */
public final class RequestConverter {

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
    final RegionSpecifier region = buildRegionSpecifier(regionName);
    return new GetRequest(region, ProtobufUtil.toGet(get, existenceOnly));
  }

  /**
   * Convert a byte array to a protocol buffer RegionSpecifier.
   *
   * @param value the region specifier byte array value
   * @return a protocol buffer RegionSpecifier
   */
  public static RegionSpecifier buildRegionSpecifier(final byte[] value) {
    return new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME, ByteBuffer.wrap(value));
  }

  /**
   * Create a protocol buffer MutateRequest for a put.
   *
   * @param regionName The region name to request from
   * @param mutation   The mutation to process
   * @param type       The type of mutation to process
   * @return a mutate request
   */
  public static MutateRequest buildMutateRequest(final byte[] regionName,
                                                 final MutationProto.MutationType type,
                                                 final Mutation mutation) {
    final RegionSpecifier region = buildRegionSpecifier(regionName);
    return new MutateRequest(region, ProtobufUtil.toMutation(type, mutation), new Condition());
  }

  /**
   * Create a protocol buffer MutateRequest for a put.
   *
   * @param regionName The region name to request from
   * @param mutation   The mutation to process
   * @param type       The type of mutation to process
   * @param condition  The optional condition or null
   * @return a mutate request
   */
  public static MutateRequest buildMutateRequest(final byte[] regionName,
                                                 final MutationProto.MutationType type,
                                                 final Mutation mutation,
                                                 final Condition condition) {
    final RegionSpecifier region = buildRegionSpecifier(regionName);
    return new MutateRequest(region,
        ProtobufUtil.toMutation(type, mutation),
        condition);
  }

  /**
   * Create a protocol buffer MultiRequest for row mutations.
   * Does not propagate Action absolute position.
   *
   * @param regionName   The region name the actions apply to.
   * @param rowMutations The row mutations to apply to the region
   * @return a data-laden RegionAction
   */
  public static RegionAction buildRegionAction(final byte[] regionName,
                                               final RowMutations rowMutations)
      throws IOException {
    final RegionSpecifier region = buildRegionSpecifier(regionName);
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
    return new RegionAction(region, true, actions);
  }
}
