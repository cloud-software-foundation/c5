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
import c5db.interfaces.C5Module;
import c5db.interfaces.C5Server;
import c5db.interfaces.ControlModule;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.interfaces.tablet.Tablet;
import c5db.messages.generated.CommandReply;
import c5db.messages.generated.ModuleSubCommand;
import c5db.messages.generated.ModuleType;
import com.google.common.util.concurrent.ListenableFuture;
import org.jetlang.channels.Request;
import org.jetlang.channels.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class MetaTabletLeaderBehavior implements TabletLeaderBehavior {

  private static final Logger LOG = LoggerFactory.getLogger(MetaTabletLeaderBehavior.class);
  private final ListenableFuture<C5Module> f;
  private final  Request<CommandRpcRequest<?>, CommandReply> request;

  public MetaTabletLeaderBehavior(final Tablet tablet, final C5Server server) {
    String metaLeader = C5ServerConstants.SET_META_LEADER + " : " + server.getNodeId();
    ModuleSubCommand moduleSubCommand = new ModuleSubCommand(ModuleType.Tablet, metaLeader);
    CommandRpcRequest<ModuleSubCommand> commandCommandRpcRequest
        = new CommandRpcRequest<>(tablet.getLeader(), moduleSubCommand);

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
      public void reply(CommandReply i) {
        LOG.info("Command Reply:", i);
      }
    };
    f = server.getModule(ModuleType.ControlRpc);
  }

  public void start() throws ExecutionException, InterruptedException {
    ControlModule controlService;
    controlService = (ControlModule) f.get();
    controlService.doMessage(request);
  }
}
