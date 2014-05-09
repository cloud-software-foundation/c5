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

import c5db.C5DB;
import c5db.C5ServerConstants;
import c5db.client.generated.RegionInfo;
import c5db.client.generated.TableName;
import c5db.control.ControlService;
import c5db.interfaces.C5Module;
import c5db.interfaces.C5Server;
import c5db.interfaces.ControlModule;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.interfaces.tablet.Tablet;
import c5db.messages.generated.CommandReply;
import c5db.messages.generated.ModuleSubCommand;
import c5db.messages.generated.ModuleType;
import c5db.util.FiberOnly;
import com.google.common.util.concurrent.ListenableFuture;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.jetlang.channels.Channel;
import org.jetlang.channels.Request;
import org.jetlang.channels.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class MetaTabletLeaderBehavior implements TabletLeaderBehavior {

  private static final Logger LOG = LoggerFactory.getLogger(MetaTabletLeaderBehavior.class);
  private final Tablet tablet;
  private final C5Server server;


  public MetaTabletLeaderBehavior(final Tablet tablet, final C5Server server) {
    this.tablet = tablet;
    this.server = server;
  }

  public void start() {
    setMetaLeaderInRootTablet();
  }

  private void setMetaLeaderInRootTablet() {
    CommandRpcRequest<ModuleSubCommand> commandCommandRpcRequest
        = new CommandRpcRequest<>(this.tablet.getLeader(),
        new ModuleSubCommand(ModuleType.Tablet, C5ServerConstants.SET_META_LEADER + " : " + server.getNodeId()));
    ListenableFuture<C5Module> f = server.getModule(ModuleType.ControlRpc);
    try {
      ControlModule controlService = (ControlModule) f.get();
      controlService.doMessage(new Request<CommandRpcRequest<?>, CommandReply>() {
        @Override
        public Session getSession() {
          LOG.info("getSession");
          return null;
        }

        @Override
        public CommandRpcRequest<?> getRequest() {
          return commandCommandRpcRequest;
        }

        @Override
        public void reply(CommandReply i) {
          LOG.info("Command Reply");
        }
      });
    } catch (InterruptedException | ExecutionException e) {
      System.exit(1);
      e.printStackTrace();
    }
  }
}