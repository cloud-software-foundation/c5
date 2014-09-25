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

package c5db.tablet;

import c5db.client.generated.Condition;
import c5db.client.generated.Get;
import c5db.client.generated.MutationProto;
import c5db.client.generated.RegionAction;
import c5db.client.generated.RegionActionResult;
import c5db.client.generated.Result;
import c5db.client.generated.Scan;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.regionserver.wal.HLog;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Our interface to a region.
 * <p>
 * Provides our abstraction to HRegion.
 */
public interface Region {

  ListenableFuture<Boolean> batchMutate(MutationProto mutateProto) throws IOException;

  /**
   * Creates instances of Region.  This exists to make mocking and testing
   * easier.
   * <p>
   * Mock out the creator interface - then create/return mock region interfaces.
   */

  boolean mutate(MutationProto mutateProto, Condition condition) throws IOException;

  boolean exists(Get get) throws IOException;

  Result get(Get get) throws IOException;

  org.apache.hadoop.hbase.regionserver.RegionScanner getScanner(Scan scan) throws IOException;

  RegionActionResult processRegionAction(RegionAction regionAction);

  boolean rowInRange(byte[] row);

  /**
   * Constructor arguments basically.
   */
  public interface Creator {
    Region getHRegion(
        Path basePath,
        HRegionInfo regionInfo,
        HTableDescriptor tableDescriptor,
        HLog log,
        Configuration conf) throws IOException;
  }
}
