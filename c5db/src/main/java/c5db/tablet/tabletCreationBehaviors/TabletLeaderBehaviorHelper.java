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
import c5db.interfaces.C5Module;
import c5db.interfaces.C5Server;
import c5db.interfaces.ControlModule;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.messages.generated.CommandReply;
import c5db.messages.generated.ModuleSubCommand;
import c5db.messages.generated.ModuleType;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetlang.channels.Request;
import org.jetlang.channels.Session;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class TabletLeaderBehaviorHelper {

  static long getLeaderFromResults(List<Cell> cells) {
    final long[] leaderNodeId = new long[1];
    cells.stream().forEach(result -> {
      if ((Arrays.equals(result.getFamily(), HConstants.CATALOG_FAMILY)) &&
          (Arrays.equals(result.getQualifier(), C5ServerConstants.LEADER_QUALIFIER))) {
        leaderNodeId[0] = Bytes.toLong(result.getValue());
      }
    });
    return leaderNodeId[0];
  }

  static void sendRequest(CommandRpcRequest<ModuleSubCommand> commandCommandRpcRequest, C5Server server)
      throws ExecutionException, InterruptedException {

    Request<CommandRpcRequest<?>, CommandReply> request = new Request<CommandRpcRequest<?>, CommandReply>() {
      @Override
      public Session getSession() {
        return null;
      }

      @Override
      public CommandRpcRequest<ModuleSubCommand> getRequest() {
        return commandCommandRpcRequest;
      }

      @Override
      public void reply(CommandReply i) {

      }
    };
    ListenableFuture<C5Module> f = server.getModule(ModuleType.ControlRpc);
    ControlModule controlService;
    controlService = (ControlModule) f.get();
    controlService.doMessage(request);
  }
}
