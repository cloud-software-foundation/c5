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
import c5db.interfaces.C5Module;
import c5db.interfaces.C5Server;
import c5db.interfaces.ControlModule;
import c5db.interfaces.TabletModule;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.interfaces.tablet.Tablet;
import c5db.messages.generated.CommandReply;
import c5db.messages.generated.ModuleSubCommand;
import c5db.messages.generated.ModuleType;
import c5db.regionserver.RegionNotFoundException;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetlang.channels.Request;
import org.jetlang.channels.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Encoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

public class UserTabletLeaderBehavior {
  private static final Logger LOG = LoggerFactory.getLogger(UserTabletLeaderBehavior.class);
  private final C5Server server;
  private final Tablet tablet;

  public UserTabletLeaderBehavior(C5Server server, Tablet tablet) {
    this.server = server;
    this.tablet = tablet;
  }

  public static String generateCommandString(long nodeId, HRegionInfo hRegionInfo){
    BASE64Encoder encoder = new BASE64Encoder();
    String hRegionInfoStr = encoder.encodeBuffer(hRegionInfo.toByteArray());
    return C5ServerConstants.SET_USER_LEADER + ":" + nodeId + "," + hRegionInfoStr;
  }

  public void start() throws ExecutionException, InterruptedException, RegionNotFoundException, IOException {
    TabletModule tabletModule = (TabletModule) server.getModule(ModuleType.Tablet).get();
    Tablet rootTablet = tabletModule.getTablet("hbase:root", ByteBuffer.wrap(new byte[0]));
    Region rootRegion = rootTablet.getRegion();

    RegionScanner scanner = rootRegion.getScanner(ProtobufUtil.toScan(new Scan()));
    ArrayList<Cell> results = new ArrayList<>();
    scanner.nextRaw(results);
    final long[] leaderNodeId = new long[1];
    results.stream().forEach(result -> {
      if ((Arrays.equals(result.getFamily(), HConstants.CATALOG_FAMILY)) &&
          (Arrays.equals(result.getQualifier(), C5ServerConstants.LEADER_QUALIFIER))) {
        leaderNodeId[0] = Bytes.toLong(result.getValue());
      }
    });

    String commandString = generateCommandString(leaderNodeId[0], this.tablet.getRegionInfo());
    ModuleSubCommand moduleSubCommand = new ModuleSubCommand(ModuleType.Tablet, commandString);
    ListenableFuture<C5Module> f;

    CommandRpcRequest<ModuleSubCommand> commandCommandRpcRequest = new CommandRpcRequest<>(leaderNodeId[0], moduleSubCommand);
    Request<CommandRpcRequest<?>, CommandReply> request;

    request = new Request<CommandRpcRequest<?>, CommandReply>() {
      @Override
      public Session getSession() {
        LOG.info("getSession");
        return null;
      }

      @Override
      public CommandRpcRequest<ModuleSubCommand> getRequest() {
        return commandCommandRpcRequest;
      }

      @Override
      public void reply(CommandReply i) {    }
    };
    f = server.getModule(ModuleType.ControlRpc);
    ControlModule controlService;
    controlService = (ControlModule) f.get();
    controlService.doMessage(request);
  }
}