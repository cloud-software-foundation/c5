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
import c5db.interfaces.ModuleInformationProvider;
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
import sun.misc.BASE64Encoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class UserTabletLeaderBehavior implements StartableTabletBehavior {
  private final ModuleInformationProvider moduleInformationProvider;
  private final HRegionInfo hRegionInfo;

  public UserTabletLeaderBehavior(ModuleInformationProvider moduleInformationProvider, HRegionInfo hRegionInfo) {
    this.moduleInformationProvider = moduleInformationProvider;
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
      TabletModule tabletModule = (TabletModule) moduleInformationProvider.getModule(ModuleType.Tablet).get();
      Tablet rootTablet = tabletModule.getTablet("hbase:root", ByteBuffer.wrap(new byte[0]));
      Region rootRegion = rootTablet.getRegion();

      RegionScanner scanner = rootRegion.getScanner(ProtobufUtil.toScan(new Scan()));
      List<Cell> results = new ArrayList<>();
      scanner.nextRaw(results);

      long leader = TabletLeaderBehaviorHelper.getLeaderFromResults(results);
      String commandString = generateCommandString(leader, hRegionInfo);
      ModuleSubCommand moduleSubCommand = new ModuleSubCommand(ModuleType.Tablet, commandString);
      CommandRpcRequest<ModuleSubCommand> commandCommandRpcRequest = new CommandRpcRequest<>(leader, moduleSubCommand);
      TabletLeaderBehaviorHelper.sendRequest(commandCommandRpcRequest, moduleInformationProvider);
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    } catch (RegionNotFoundException e) {
      throw new IOException(e);
    }
  }
}