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
package c5db;

import c5db.interfaces.C5Server;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.messages.generated.ModuleType;
import c5db.messages.generated.StartModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.Random;

/**
 * CLI Entry point for the C5DB server.
 */
public class Main {
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws Exception {
    C5Server instance = startC5Server(args);

    instance.getShutdownFuture().get();
  }

  public static C5Server startC5Server(String[] args) throws Exception {
    String username = System.getProperty("user.name");

    // nodeId is random initially.  Then if provided on args, we take that.
    Random nodeIdRandomizer = new Random();
    long nodeId = nodeIdRandomizer.nextLong();

    if (args.length > 0) {
      nodeId = Long.parseLong(args[0]);
    }

    String cfgPath = "/tmp/" + username + "/c5-" + Long.toString(nodeId);

    // use system properties for other config so we don't end up writing a whole command line
    // parse framework.
    String reqCfgPath = System.getProperty(C5ServerConstants.C5_CFG_PATH);
    if (reqCfgPath != null) {
      cfgPath = reqCfgPath;
    }

    ConfigDirectory cfgDir = new NioFileConfigDirectory(Paths.get(cfgPath));
    cfgDir.setNodeIdFile(Long.toString(nodeId));

    C5Server instance = new C5DB(cfgDir);
    instance.start();
    Random portRandomizer = new Random();

    int regionServerPort;
    if (hasPropertyPortSet()) {
      regionServerPort = getPropertyPort();
    } else {
      regionServerPort = C5ServerConstants.DEFAULT_REGION_SERVER_PORT_MIN
          + portRandomizer.nextInt(C5ServerConstants.REGION_SERVER_PORT_RANGE);
    }

    int webServerPort;
    if (hasWebServerPropertyPortSet()) {
      webServerPort = getWebServerPropertyPort();
    } else {
      webServerPort = C5ServerConstants.DEFAULT_WEB_SERVER_PORT;
    }


    // issue startup commands here that are common/we always want:
    StartModule startLog = new StartModule(ModuleType.Log, 0, "");
    instance.getCommandChannel().publish(new CommandRpcRequest<>(nodeId, startLog));

    StartModule startBeacon = new StartModule(ModuleType.Discovery, C5ServerConstants.DISCOVERY_PORT, "");
    instance.getCommandChannel().publish(new CommandRpcRequest<>(nodeId, startBeacon));

    StartModule startReplication = new StartModule(ModuleType.Replication,
        portRandomizer.nextInt(C5ServerConstants.REPLICATOR_PORT_RANGE)
            + C5ServerConstants.REPLICATOR_PORT_MIN, ""
    );
    instance.getCommandChannel().publish(new CommandRpcRequest<>(nodeId, startReplication));

    StartModule startTablet = new StartModule(ModuleType.Tablet, 0, "");
    instance.getCommandChannel().publish(new CommandRpcRequest<>(nodeId, startTablet));

    StartModule startRegionServer = new StartModule(ModuleType.RegionServer, regionServerPort, "");
    instance.getCommandChannel().publish(new CommandRpcRequest<>(nodeId, startRegionServer));

    StartModule webAdminService = new StartModule(ModuleType.WebAdmin, webServerPort, "");
    instance.getCommandChannel().publish(new CommandRpcRequest<>(nodeId, webAdminService));
    return instance;
  }

  private static int getPropertyPort() {
    return Integer.parseInt(System.getProperty(C5ServerConstants.REGION_SERVER_PORT_PROPERTY_NAME));
  }

  private static int getWebServerPropertyPort() {
    return Integer.parseInt(System.getProperty(C5ServerConstants.WEB_SERVER_PORT_PROPERTY_NAME));
  }

  private static boolean hasPropertyPortSet() {
    return System.getProperties().containsKey(C5ServerConstants.REGION_SERVER_PORT_PROPERTY_NAME);
  }

  private static boolean hasWebServerPropertyPortSet() {
    return System.getProperties().containsKey(C5ServerConstants.WEB_SERVER_PORT_PROPERTY_NAME);
  }
}
