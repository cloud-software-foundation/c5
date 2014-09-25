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

package c5db.tablet.tabletCreationBehaviors;

import c5db.C5ServerConstants;
import c5db.client.ProtobufUtil;
import c5db.client.generated.Condition;
import c5db.client.generated.MutationProto;
import c5db.client.generated.RegionInfo;
import c5db.client.generated.Scan;
import c5db.client.generated.TableName;
import c5db.interfaces.ModuleInformationProvider;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.interfaces.tablet.Tablet;
import c5db.messages.generated.ModuleSubCommand;
import c5db.messages.generated.ModuleType;
import c5db.tablet.Region;
import c5db.tablet.SystemTableNames;
import c5db.util.FiberOnly;
import c5db.util.TabletNameHelpers;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class RootTabletLeaderBehavior implements TabletLeaderBehavior {
  private final long numberOfMetaPeers;
  private final Tablet tablet;
  private final ModuleInformationProvider moduleInformationProvider;

  public RootTabletLeaderBehavior(final Tablet tablet,
                                  final ModuleInformationProvider moduleInformationProvider,
                                  final long numberOfMetaPeers) {
    this.numberOfMetaPeers = numberOfMetaPeers;
    this.moduleInformationProvider = moduleInformationProvider;
    this.tablet = tablet;
  }

  public void start() throws IOException, ExecutionException, InterruptedException {
    Region region = tablet.getRegion();
    if (!metaExists(region)) {
      List<Long> pickedPeers = shuffleListAndReturnMetaRegionPeers(tablet.getPeers());
      createLeaderLessMetaEntryInRoot(region, pickedPeers);
      requestMetaCommandCreated(pickedPeers);
    } else {
      // TODO Check to see if you can take root
    }
  }

  boolean metaExists(Region region) throws IOException {
    // TODO We should make sure the meta is well formed
    RegionScanner scanner = region.getScanner(new Scan());
    List<Cell> results = new ArrayList<>();
    scanner.next(results, 1);
    return results.size() > 0;
  }

  private List<Long> shuffleListAndReturnMetaRegionPeers(final List<Long> peers) {
    Collections.shuffle(new ArrayList<>(peers));
    return peers.subList(0, (int) numberOfMetaPeers);
  }

  @FiberOnly
  private void requestMetaCommandCreated(List<Long> peers) throws ExecutionException, InterruptedException {
    String pickedPeersString = StringUtils.join(peers, ',');
    ModuleSubCommand moduleSubCommand = new ModuleSubCommand(ModuleType.Tablet,
        C5ServerConstants.START_META + ":" + pickedPeersString);

    for (long peer : peers) {
      CommandRpcRequest<ModuleSubCommand> commandRpcRequest = new CommandRpcRequest<>(peer, moduleSubCommand);
      TabletLeaderBehaviorHelper.sendRequest(commandRpcRequest, moduleInformationProvider);
    }
  }

  private void createLeaderLessMetaEntryInRoot(Region region, List<Long> pickedPeers) throws IOException {
    org.apache.hadoop.hbase.TableName hbaseDatabaseName = SystemTableNames.metaTableName();
    ByteBuffer hbaseNameSpace = ByteBuffer.wrap(hbaseDatabaseName.getNamespace());
    ByteBuffer hbaseTableName = ByteBuffer.wrap(hbaseDatabaseName.getQualifier());
    TableName tableName = new TableName(hbaseNameSpace, hbaseTableName);

    byte[] initialMetaRockeyInRoot = Bytes.add(TabletNameHelpers.toBytes(tableName), SystemTableNames.sep, new byte[0]);
    Put put = new Put(initialMetaRockeyInRoot);

    RegionInfo regionInfo = new RegionInfo(1,
        tableName,
        pickedPeers,
        0l, // This signifies that we haven't picked the leader
        ByteBuffer.wrap(new byte[0]),
        ByteBuffer.wrap(new byte[0]),
        true,
        false);

    put.add(HConstants.CATALOG_FAMILY,
        HConstants.REGIONINFO_QUALIFIER,
        ProtobufIOUtil.toByteArray(regionInfo, RegionInfo.getSchema(), LinkedBuffer.allocate(512)));

    boolean processed = region.mutate(ProtobufUtil.toMutation(MutationProto.MutationType.PUT, put), new Condition());
    if (!processed) {
      throw new IOException("Unable to mutate root and thus we can't properly run root tablet leader behavior");
    }
  }
}
