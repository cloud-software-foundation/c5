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
import c5db.client.generated.RegionInfo;
import c5db.client.generated.TableName;
import c5db.interfaces.C5Server;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.interfaces.tablet.Tablet;
import c5db.messages.generated.ModuleSubCommand;
import c5db.messages.generated.ModuleType;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.jetlang.channels.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RootTabletLeaderBehavior implements TabletLeaderBehavior {

  private static final Logger LOG = LoggerFactory.getLogger(RootTabletLeaderBehavior.class);
  private final c5db.interfaces.tablet.Tablet tablet;
  private final C5Server server;

  public RootTabletLeaderBehavior(final Tablet tablet,
                                  final C5Server server) {
    this.tablet = tablet;
    this.server = server;
  }

  private void bootStrapMeta(Region region, List<Long> peers) throws IOException {
    List<Long> pickedPeers = pickPeers(peers);
    long leader = pickLeader(pickedPeers);
    createMetaEntryInRoot(region, pickedPeers, leader);
    requestMetaCommandCreated(pickedPeers, leader);
  }

  private void requestMetaCommandCreated(List<Long> pickedPeers, long leader) {
    String pickedPeersString = StringUtils.join(pickedPeers, ',');
    ModuleSubCommand moduleSubCommand = new ModuleSubCommand(ModuleType.Tablet,
        C5ServerConstants.START_META + ":" + pickedPeersString);
    CommandRpcRequest<ModuleSubCommand> commandRpcRequest = new CommandRpcRequest<>(leader, moduleSubCommand);
    Channel<CommandRpcRequest<?>> channel = server.getCommandChannel();
    channel.publish(commandRpcRequest);

  }

  private void createMetaEntryInRoot(Region region, List<Long> pickedPeers, long leader) throws IOException {
    Put put = new Put(C5ServerConstants.META_ROW);
    TableName tableName = new TableName(ByteBuffer.wrap(C5ServerConstants.INTERNAL_NAMESPACE),
        ByteBuffer.wrap(C5ServerConstants.META_TABLE_NAME));
    RegionInfo regionInfo = new RegionInfo(1,
        tableName,
        pickedPeers,
        leader,
        ByteBuffer.wrap(C5ServerConstants.META_START_KEY),
        ByteBuffer.wrap(C5ServerConstants.META_END_KEY),
        true,
        false);
    put.add(HConstants.CATALOG_FAMILY,
        HConstants.REGIONINFO_QUALIFIER,
        ProtobufIOUtil.toByteArray(regionInfo, RegionInfo.getSchema(), LinkedBuffer.allocate(512)));
    region.put(put);
  }

  private long pickLeader(List<Long> pickedPeers) {
    if (server.isSingleNodeMode()) {
      if (pickedPeers.size() > 1) {
        LOG.error("We are in single mode but we have multiple peers");
        throw new UnsupportedOperationException("We are in single mode but we have multiple peers");
      }
    }
    return server.getNodeId();
  }

  private List<Long> pickPeers(List<Long> peers) {
    if (server.isSingleNodeMode()) {
      if (peers.size() > 1) {
        LOG.error("We are in single mode but we have multiple peers");
        throw new UnsupportedOperationException("We are in single mode but we have multiple peers");
      }
      return Arrays.asList(peers.iterator().next());
    } else {
      if (peers.size() >= 3){
        List<Long> peersCopy = new ArrayList(peers);
        Collections.shuffle(peersCopy);
        List<Long> peersToReturn = new ArrayList<>();
        peersToReturn.add(server.getNodeId());

        int counter = 0;
        while (peersToReturn.size() < 3 && counter < peersCopy.size() ){
          if (!peersToReturn.contains(peersCopy.get(counter))){
            peersToReturn.add(peersCopy.get(counter));
          }
          counter++;
        }
        if (peersToReturn.size() == 3) {
          return peersToReturn;
        } else {
          throw new UnsupportedOperationException("Unable to track down enough nodes to make progress");
        }
      } else{
        throw new UnsupportedOperationException("Unable to track down enough nodes to make progress");
      }
    }
  }

  boolean getExists(Region region, Get get) throws IOException {
    Result result = region.get(get);
    return !(result == null) && result.size() > 0;
  }

  boolean metaExists(Region region) throws IOException {
    // TODO We should make sure the meta is well formed
    Get get = new Get(C5ServerConstants.META_ROW);
    return getExists(region, get);
  }

  public void start() throws IOException {
    Region region = tablet.getRegion();
    if (!metaExists(region)) {
      List<Long> peers = tablet.getPeers();
      bootStrapMeta(region, peers);
    }
  }
}
