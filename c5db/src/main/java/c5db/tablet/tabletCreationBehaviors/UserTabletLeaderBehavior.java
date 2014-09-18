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
import c5db.interfaces.C5Server;
import c5db.interfaces.TabletModule;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.interfaces.tablet.Tablet;
import c5db.messages.generated.ModuleSubCommand;
import c5db.messages.generated.ModuleType;
import c5db.regionserver.RegionNotFoundException;
import c5db.tablet.Region;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Encoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class UserTabletLeaderBehavior implements StartableTabletBehavior {
  private static final Logger LOG = LoggerFactory.getLogger(UserTabletLeaderBehavior.class);
  private final C5Server server;
  private final HRegionInfo hRegionInfo;

  public UserTabletLeaderBehavior(C5Server server, HRegionInfo hRegionInfo) {
    this.server = server;
    this.hRegionInfo = hRegionInfo;
  }

  private static String generateCommandString(long nodeId, HRegionInfo hRegionInfo) {
    BASE64Encoder encoder = new BASE64Encoder();
    String hRegionInfoStr = encoder.encodeBuffer(hRegionInfo.toByteArray());
    return C5ServerConstants.SET_USER_LEADER + ":" + nodeId + "," + hRegionInfoStr;
  }

  @Override
  public void start() throws InterruptedException, IOException {
    try {
      TabletModule tabletModule = (TabletModule) server.getModule(ModuleType.Tablet).get();
      Tablet rootTablet = tabletModule.getTablet("hbase:root", ByteBuffer.wrap(new byte[0]));
      Region rootRegion = rootTablet.getRegion();

      RegionScanner scanner = rootRegion.getScanner(ProtobufUtil.toScan(new Scan()));
      List<Cell> results = new ArrayList<>();
      scanner.nextRaw(results);

      long leader = TabletLeaderBehaviorHelper.getLeaderFromResults(results);
      String commandString = generateCommandString(leader, hRegionInfo);
      ModuleSubCommand moduleSubCommand = new ModuleSubCommand(ModuleType.Tablet, commandString);
      CommandRpcRequest<ModuleSubCommand> commandCommandRpcRequest = new CommandRpcRequest<>(leader, moduleSubCommand);
      TabletLeaderBehaviorHelper.sendRequest(commandCommandRpcRequest, server);
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    } catch (RegionNotFoundException e) {
      throw new IOException(e);
    }
  }
}