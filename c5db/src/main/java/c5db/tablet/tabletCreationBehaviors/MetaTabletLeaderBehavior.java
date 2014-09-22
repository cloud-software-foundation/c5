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
import c5db.interfaces.C5Server;
import c5db.interfaces.TabletModule;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.interfaces.tablet.Tablet;
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

    long leader = rootTablet.getLeader();
    CommandRpcRequest<ModuleSubCommand> commandCommandRpcRequest = new CommandRpcRequest<>(leader, moduleSubCommand);
    TabletLeaderBehaviorHelper.sendRequest(commandCommandRpcRequest, server);
  }
}