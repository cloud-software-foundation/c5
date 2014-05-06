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
import c5db.util.FiberOnly;
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
import java.util.Collections;
import java.util.List;

public class RootTabletLeaderBehavior implements TabletLeaderBehavior {

  private static final Logger LOG = LoggerFactory.getLogger(RootTabletLeaderBehavior.class);
  private final c5db.interfaces.tablet.Tablet tablet;
  private final C5Server server;
  private final long numberOfMetaPeers;

  public RootTabletLeaderBehavior(final Tablet tablet,
                                  final C5Server server,
                                  final long numberOfMetaPeers) {
    this.tablet = tablet;
    this.server = server;
    this.numberOfMetaPeers = numberOfMetaPeers;
  }

  @FiberOnly
  private void bootStrapMeta(Region region, List<Long> peers) throws IOException {
    List<Long> pickedPeers = pickPeers(peers);
    requestMetaCommandCreated(pickedPeers);
    createLeaderLessMetaEntryInRoot(region, pickedPeers);
  }

  @FiberOnly
  private void requestMetaCommandCreated(List<Long> peers) {
    String pickedPeersString = StringUtils.join(peers, ',');
    ModuleSubCommand moduleSubCommand = new ModuleSubCommand(ModuleType.Tablet,
        C5ServerConstants.START_META + ":" + pickedPeersString);
    Channel<CommandRpcRequest<?>> channel = server.getCommandChannel();

    for (Long peer : peers) {
      CommandRpcRequest<ModuleSubCommand> commandRpcRequest = new CommandRpcRequest<>(peer, moduleSubCommand);
      channel.publish(commandRpcRequest);
    }
  }

  private void createLeaderLessMetaEntryInRoot(Region region, List<Long> pickedPeers) throws IOException {
    Put put = new Put(C5ServerConstants.META_ROW);
    TableName tableName = new TableName(ByteBuffer.wrap(C5ServerConstants.INTERNAL_NAMESPACE),
        ByteBuffer.wrap(C5ServerConstants.META_TABLE_NAME));
    RegionInfo regionInfo = new RegionInfo(1,
        tableName,
        pickedPeers,
        0l, // This signifies that we haven't picked the leader
        // TBD The meta leader updates the entry with itself
        ByteBuffer.wrap(C5ServerConstants.META_START_KEY),
        ByteBuffer.wrap(C5ServerConstants.META_END_KEY),
        true,
        false);
    put.add(HConstants.CATALOG_FAMILY,
        HConstants.REGIONINFO_QUALIFIER,
        ProtobufIOUtil.toByteArray(regionInfo, RegionInfo.getSchema(), LinkedBuffer.allocate(512)));
    region.put(put);
  }

  private List<Long> pickPeers(List<Long> peers) {
    List<Long> peersCopy = new ArrayList<>(peers);
    Collections.shuffle(peersCopy);
    List<Long> peersToReturn = new ArrayList<>();

    int counter = 0;
    while (peersToReturn.size() < numberOfMetaPeers
        && counter < peersCopy.size()) {
      if (!peersToReturn.contains(peersCopy.get(counter))) {
        peersToReturn.add(peersCopy.get(counter));
      }
      counter++;
    }
    if (peersToReturn.size() == numberOfMetaPeers) {
      return peersToReturn;
    } else {
      throw new UnsupportedOperationException("Unable to track down enough nodes to make progress");
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
