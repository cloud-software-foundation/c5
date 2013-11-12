/*
 * Copyright (C) 2013  Ohm Data
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
package ohmdb.tablet;

import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import ohmdb.OnlineRegions;
import ohmdb.interfaces.OhmModule;
import ohmdb.interfaces.OhmServer;
import ohmdb.interfaces.ReplicationModule;
import ohmdb.interfaces.TabletModule;
import ohmdb.regionserver.RegistryFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;

import java.nio.file.Path;

import static ohmdb.OhmStatic.bootStrapRegions;
import static ohmdb.OhmStatic.existingRegister;
import static ohmdb.OhmStatic.recoverOhmServer;
import static ohmdb.log.OLog.moveAwayOldLogs;
import static ohmdb.messages.ControlMessages.ModuleType;

/**
 *
 */
public class TabletService extends AbstractService implements TabletModule {
    private final PoolFiberFactory fiberFactory;
    private final Fiber fiber;
    private final OhmServer server;
    // TODO bring this into this class, and not have an external class.
    private final OnlineRegions onlineRegions = OnlineRegions.INSTANCE;
    private ReplicationModule replicationModule;

    public TabletService(PoolFiberFactory fiberFactory, OhmServer server) {
        this.fiberFactory = fiberFactory;
        this.fiber = fiberFactory.create();
        this.server = server;

    }



    @Override
    protected void doStart() {
        this.fiber.start();

        // we need a handle on the replicator service now:
        ListenableFuture<OhmModule> replicatorService = server.getModule(ModuleType.Replication);
        Futures.addCallback(replicatorService, new FutureCallback<OhmModule>() {
            @Override
            public void onSuccess(OhmModule result) {
                replicationModule = (ReplicationModule) result;
                // run something on the fiber to get bootstrapped:
                fiber.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {


                            Configuration conf = HBaseConfiguration.create();

                            Path path = server.getConfigDirectory().baseConfigPath;


                            RegistryFile registryFile;

                            registryFile = new RegistryFile(path);

                            // TODO these functions should be part of the tablet service.
                            if (existingRegister(registryFile)) {
                                recoverOhmServer(conf, path, registryFile);
                            } else {
                                bootStrapRegions(conf, path, registryFile);
                            }



                            notifyStarted();
                        } catch (Exception e) {
                            notifyFailed(e);
                        }
                    }
                });
            }

            @Override
            public void onFailure(Throwable t) {
                notifyFailed(t);
            }
        });


    }

    @Override
    protected void doStop() {
        this.fiber.dispose();
        notifyStopped();
    }



    private final Channel<TabletStateChange> tabletStateChangeChannel = new MemoryChannel<>();

    @Override
    public Channel<TabletStateChange> getTabletStateChanges() {
        return tabletStateChangeChannel;
    }

    @Override
    public ModuleType getModuleType() {
        return ModuleType.Tablet;
    }

    @Override
    public boolean hasPort() {
        return false;
    }

    @Override
    public int port() {
        return 0;
    }
}
