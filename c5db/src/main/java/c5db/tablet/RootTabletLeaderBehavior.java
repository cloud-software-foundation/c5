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
package c5db.tablet;

import c5db.C5ServerConstants;
import c5db.client.ProtobufUtil;
import c5db.client.generated.Column;
import c5db.client.generated.Condition;
import c5db.client.generated.Filter;
import c5db.client.generated.Get;
import c5db.client.generated.MutationProto;
import c5db.client.generated.NameBytesPair;
import c5db.client.generated.RegionInfo;
import c5db.client.generated.Result;
import c5db.client.generated.TableName;
import c5db.client.generated.TimeRange;
import c5db.interfaces.C5Server;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.interfaces.tablet.Tablet;
import c5db.messages.generated.ModuleSubCommand;
import c5db.messages.generated.ModuleType;
import c5db.util.FiberOnly;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.jetlang.channels.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RootTabletLeaderBehavior implements TabletLeaderBehavior {

  private static final Logger LOG = LoggerFactory.getLogger(RootTabletLeaderBehavior.class);
  private final long numberOfMetaPeers;
  private final Tablet tablet;
  Channel<CommandRpcRequest<?>> commandRpcRequestChannel;

  public RootTabletLeaderBehavior(final Tablet tablet,
                                  final C5Server server,
                                  final long numberOfMetaPeers) {
    this.numberOfMetaPeers = numberOfMetaPeers;
    commandRpcRequestChannel = server.getCommandChannel();
    this.tablet = tablet;

  }

  public void start() throws IOException {

    Region region = tablet.getRegion();
    if (!metaExists(region)) {
      List<Long> pickedPeers = shuffleListAndReturnMetaRegionPeers(tablet.getPeers());
      createLeaderLessMetaEntryInRoot(region, pickedPeers);
      requestMetaCommandCreated(pickedPeers);
    } else {
      // Check to see if you can take root
    }
 }

  boolean metaExists(Region region) {
    // TODO We should make sure the meta is well formed
    org.apache.hadoop.hbase.client.Get get = new org.apache.hadoop.hbase.client.Get(C5ServerConstants.META_ROW);
    try {
      return region.exists(ProtobufUtil.toGet(get, true));
    } catch (IOException e) {
      return false;
    }
  }

  private List<Long> shuffleListAndReturnMetaRegionPeers(final List<Long> peers) {
    Collections.shuffle(new ArrayList<>(peers));
    return peers.subList(0, (int)numberOfMetaPeers);
  }

  @FiberOnly
  private void requestMetaCommandCreated(List<Long> peers) {
    String pickedPeersString = StringUtils.join(peers, ',');
    ModuleSubCommand moduleSubCommand = new ModuleSubCommand(ModuleType.Tablet,
        C5ServerConstants.START_META + ":" + pickedPeersString);

    CommandRpcRequest<ModuleSubCommand> commandRpcRequest = new CommandRpcRequest<>(peers.get(0), moduleSubCommand);
    commandRpcRequestChannel.publish(commandRpcRequest);

  }
  private void createLeaderLessMetaEntryInRoot(Region region, List<Long> pickedPeers) throws IOException {
    Put put = new Put(C5ServerConstants.META_ROW);
    org.apache.hadoop.hbase.TableName hbaseDatabaseName = SystemTableNames.metaTableName();
    ByteBuffer hbaseNameSpace = ByteBuffer.wrap(hbaseDatabaseName.getNamespace());
    ByteBuffer hbaseTableName = ByteBuffer.wrap(hbaseDatabaseName.getName());
    TableName tableName = new TableName(hbaseNameSpace, hbaseTableName);

    ByteBuffer startKey = ByteBuffer.wrap(C5ServerConstants.META_START_KEY);
    ByteBuffer endKey = ByteBuffer.wrap(C5ServerConstants.META_END_KEY);

    RegionInfo regionInfo = new RegionInfo(1,
        tableName,
        pickedPeers,
        0l, // This signifies that we haven't picked the leader
        startKey,
        endKey,
        true,
        false);

    put.add(HConstants.CATALOG_FAMILY,
        HConstants.REGIONINFO_QUALIFIER,
        ProtobufIOUtil.toByteArray(regionInfo, RegionInfo.getSchema(), LinkedBuffer.allocate(512)));
    region.mutate(ProtobufUtil.toMutation(MutationProto.MutationType.PUT, put), new Condition());
  }
}
