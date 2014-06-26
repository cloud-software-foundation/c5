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
import c5db.interfaces.TabletModule;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.interfaces.tablet.Tablet;
import c5db.messages.generated.CommandReply;
import c5db.messages.generated.ModuleSubCommand;
import c5db.messages.generated.ModuleType;
import c5db.regionserver.RegionNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

public class MetaTabletLeaderBehavior implements TabletLeaderBehavior {
  private static final Logger LOG = LoggerFactory.getLogger(MetaTabletLeaderBehavior.class);
  private final C5Server server;

  public MetaTabletLeaderBehavior(final C5Server server) {
    this.server = server;
  }

  public void start() throws ExecutionException, InterruptedException, RegionNotFoundException {
    TabletModule tabletModule = (TabletModule) server.getModule(ModuleType.Tablet).get();
    Tablet rootTablet = tabletModule.getTablet("hbase:root", ByteBuffer.wrap(new byte[0]));
    String metaLeader = C5ServerConstants.SET_META_LEADER + ":" + server.getNodeId();
    ModuleSubCommand moduleSubCommand = new ModuleSubCommand(ModuleType.Tablet, metaLeader);
    CommandRpcRequest<ModuleSubCommand> commandCommandRpcRequest
        = new CommandRpcRequest<>(rootTablet.getLeader(), moduleSubCommand);
    TabletLeaderBehaviorHelper.sendRequest(commandCommandRpcRequest, server);
  }
}