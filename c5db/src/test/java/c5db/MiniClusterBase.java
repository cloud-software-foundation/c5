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

import c5db.C5DB;
import c5db.interfaces.C5Module;
import c5db.interfaces.C5Server;
import c5db.messages.generated.ModuleType;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mortbay.log.Log;

import java.io.IOException;
import java.util.Random;

public class MiniClusterBase {
  private static int regionServerPort;
  static boolean initialized = false;
  private static Random rnd = new Random();

  public static int getRegionServerPort() throws InterruptedException {
    while (initialized = false){
      Thread.sleep(100);
    }
    return regionServerPort;
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    Log.warn("-----------------------------------------------------------------------------------------------------------");


    regionServerPort = 8080 + rnd.nextInt(1000);
    System.setProperty("singleNode", "true");
    System.setProperty("regionServerPort", String.valueOf(regionServerPort));

    C5DB.main(new String[]{});
    C5Server server = C5DB.getServer();

    ListenableFuture<C5Module> regionServerFuture = server.getModule(ModuleType.RegionServer);
    C5Module regionServer = regionServerFuture.get();

    ListenableFuture<C5Module> tabletServerFuture = server.getModule(ModuleType.Tablet);
    C5Module tabletServer = tabletServerFuture.get();

    ListenableFuture<C5Module> replicationServerFuture = server.getModule(ModuleType.Replication);
    C5Module replicationServer = replicationServerFuture .get();

    while (!regionServer.isRunning() ||
        !tabletServer.isRunning() ||
        !replicationServer.isRunning()){
      Thread.sleep(100);
    }
    Thread.sleep(4500);
    initialized = true;
  }


}
