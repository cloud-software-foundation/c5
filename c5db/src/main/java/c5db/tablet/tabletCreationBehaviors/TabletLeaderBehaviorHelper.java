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
import c5db.interfaces.C5Module;
import c5db.interfaces.ControlModule;
import c5db.interfaces.ModuleInformationProvider;
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

class TabletLeaderBehaviorHelper {

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

  static void sendRequest(CommandRpcRequest<ModuleSubCommand> commandCommandRpcRequest,
                          ModuleInformationProvider moduleInformationProvider)
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
    ListenableFuture<C5Module> f = moduleInformationProvider.getModule(ModuleType.ControlRpc);
    ControlModule controlService;
    controlService = (ControlModule) f.get();
    controlService.doMessage(request);
  }
}
