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
import c5db.interfaces.TabletModule;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.jetlang.channels.Channel;
import org.jetlang.fibers.Fiber;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class RootTabletLeaderBehavior implements TabletLeaderBehavior {

  private final Fiber fiber;
  private final TabletModule.Tablet tablet;
  private final C5Server server;

  public RootTabletLeaderBehavior(final Fiber fiber,
                                  final TabletModule.Tablet tablet,
                                  final C5Server server,
                                  final Channel<String> onComplete) {
    this.fiber = fiber;
    this.tablet = tablet;
    this.server = server;
    fiber.execute(() -> {
      Region region = tablet.getRegion();
      try {
        if (!metaExists(region)) {
          List<Long> peers = tablet.getPeers();
          bootStrapMeta(region, peers);
        }
      } catch (IOException e) {
        //TODO Throwing in an exception is evil, but this will never happen
        // unit you start the fiber.
        throw new RuntimeException("Unable To bootstrap meta");
      }
    });

    onComplete.publish("Complete");
  }

  private void bootStrapMeta(Region region, List<Long> peers) throws IOException {
    List<Long> pickedPeers = pickPeers(peers);
    long leader = pickLeader(pickedPeers);
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
    put.add(C5ServerConstants.META_INFO_CF,
        C5ServerConstants.META_INFO_CQ,
        ProtobufIOUtil.toByteArray(regionInfo, RegionInfo.getSchema(), LinkedBuffer.allocate(512)));
    region.put(put);
  }

  private long pickLeader(List<Long> pickedPeers) {
    if (server.isSingleNodeMode()) {
      return 1;
    } else {
      throw new UnsupportedOperationException("we only support single node currently");
    }
  }

  private List<Long> pickPeers(List<Long> peers) {
    return Arrays.asList(1l);
  }

  boolean getExists(Region region, Get get) throws IOException {
    Result result = region.get(get);
    return result.size() > 0;
  }

  boolean metaExists(Region region) throws IOException {
    // TODO We should make sure the meta is well formed
    Get get = new Get(C5ServerConstants.META_ROW);
    return getExists(region, get);
  }

  public void start() {
    fiber.start();
  }


}
