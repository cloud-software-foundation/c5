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
package c5db.tablet.tabletCreationBehaviors;

import c5db.C5ServerConstants;
import c5db.client.ProtobufUtil;
import c5db.client.generated.Condition;
import c5db.client.generated.MutationProto;
import c5db.client.generated.RegionInfo;
import c5db.client.generated.Scan;
import c5db.client.generated.TableName;
import c5db.interfaces.C5Server;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class RootTabletLeaderBehavior implements TabletLeaderBehavior {

  private static final Logger LOG = LoggerFactory.getLogger(RootTabletLeaderBehavior.class);
  private final long numberOfMetaPeers;
  private final Tablet tablet;
  private final C5Server server;

  public RootTabletLeaderBehavior(final Tablet tablet,
                                  final C5Server server) {
    this.numberOfMetaPeers = server.isSingleNodeMode() ? 1 : C5ServerConstants.DEFAULT_QUORUM_SIZE;
    this.server = server;
    this.tablet = tablet;
  }

  public void start() throws IOException, ExecutionException, InterruptedException {

    while (tablet.getLeader() == 0){
      LOG.info("Sleeping for a second waiting to become the leader");
      Thread.sleep(1000);
    }

    Region region = tablet.getRegion();
    if (!metaExists(region)) {
      List<Long> pickedPeers = shuffleListAndReturnMetaRegionPeers(tablet.getPeers());
          createLeaderLessMetaEntryInRoot(region, pickedPeers);
      requestMetaCommandCreated(pickedPeers);
    } else {
      // Check to see if you can take root
    }
  }

  boolean metaExists(Region region) throws IOException {
    // TODO We should make sure the meta is well formed
    RegionScanner scanner = region.getScanner(new Scan());
    ArrayList<Cell> results = new ArrayList<>();
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
      TabletLeaderBehaviorHelper.sendRequest(commandRpcRequest, server);
    }
  }
  private void createLeaderLessMetaEntryInRoot(Region region, List<Long> pickedPeers) throws IOException {
    org.apache.hadoop.hbase.TableName hbaseDatabaseName = SystemTableNames.metaTableName();
    ByteBuffer hbaseNameSpace = ByteBuffer.wrap(hbaseDatabaseName.getNamespace());
    ByteBuffer hbaseTableName = ByteBuffer.wrap(hbaseDatabaseName.getQualifier());
    TableName tableName = new TableName(hbaseNameSpace, hbaseTableName);

    byte[] initalMetaRowkeyInRoot = Bytes.add(TabletNameHelpers.toBytes(tableName), SystemTableNames.sep, new byte[0]);
    Put put = new Put(initalMetaRowkeyInRoot);

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
    if (!processed){
      throw new IOException("Unable to set root");
    }
  }
}
