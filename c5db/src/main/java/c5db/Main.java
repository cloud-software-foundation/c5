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

package c5db;

import c5db.interfaces.C5Server;
import c5db.interfaces.ControlModule;
import c5db.interfaces.DiscoveryModule;
import c5db.interfaces.LogModule;
import c5db.interfaces.RegionServerModule;
import c5db.interfaces.ReplicationModule;
import c5db.interfaces.TabletModule;
import c5db.interfaces.WebAdminModule;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.messages.generated.ModuleType;
import c5db.messages.generated.StartModule;
import c5db.module_cfg.ModuleDeps;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.Map;
import java.util.Random;
import java.util.Set;

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

    int controlRpcServerPort;
    if (hasControlRpcPropertyPortSet()) {
      controlRpcServerPort = getControlRpcPropertyPortSet();
    } else {
      controlRpcServerPort = C5ServerConstants.CONTROL_RPC_PROPERTY_PORT;
    }

    int replicationPort = portRandomizer.nextInt(C5ServerConstants.REPLICATOR_PORT_RANGE)
        + C5ServerConstants.REPLICATOR_PORT_MIN;

    C5Server instance = new C5DB(nodeId);
    instance.start();

    // issue startup commands here that are common/we always want:

    Set<Class<?>> modulesToStart = Sets.newHashSet(
        LogModule.class,
        DiscoveryModule.class,
        ReplicationModule.class,
        TabletModule.class,
        RegionServerModule.class,
        WebAdminModule.class,
        ControlModule.class);

    Map<ModuleType, Integer> modulePorts = new ImmutableMap.Builder<ModuleType, Integer>()
        .put(ModuleType.Log, 0)
        .put(ModuleType.Discovery, C5ServerConstants.DISCOVERY_PORT)
        .put(ModuleType.Replication, replicationPort)
        .put(ModuleType.Tablet, 0)
        .put(ModuleType.RegionServer, regionServerPort)
        .put(ModuleType.WebAdmin, webServerPort)
        .put(ModuleType.ControlRpc, controlRpcServerPort)
        .build();

    for (ModuleType moduleType : ModuleDeps.getModuleReverseDependencyOrder(modulesToStart)) {
      int port = modulePorts.get(moduleType);
      StartModule startModuleMessage = new StartModule(moduleType, port, "");
      instance.getCommandChannel().publish(new CommandRpcRequest<>(nodeId, startModuleMessage));
    }

    return instance;
  }


  private static int getPropertyPort() {
    return Integer.parseInt(System.getProperty(C5ServerConstants.REGION_SERVER_PORT_PROPERTY_NAME));
  }

  private static int getWebServerPropertyPort() {
    return Integer.parseInt(System.getProperty(C5ServerConstants.WEB_SERVER_PORT_PROPERTY_NAME));
  }

  private static int getControlRpcPropertyPortSet() {
    return Integer.parseInt(System.getProperty(C5ServerConstants.CONTROL_SERVER_PORT_PROPERTY_NAME));
  }

  private static boolean hasPropertyPortSet() {
    return System.getProperties().containsKey(C5ServerConstants.REGION_SERVER_PORT_PROPERTY_NAME);
  }

  private static boolean hasWebServerPropertyPortSet() {
    return System.getProperties().containsKey(C5ServerConstants.WEB_SERVER_PORT_PROPERTY_NAME);
  }

  private static boolean hasControlRpcPropertyPortSet() {
    return System.getProperties().containsKey(C5ServerConstants.CONTROL_SERVER_PORT_PROPERTY_NAME);
  }
}
