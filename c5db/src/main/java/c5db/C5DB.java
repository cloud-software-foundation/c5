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

import c5db.interfaces.C5Module;
import c5db.interfaces.C5Server;
import c5db.log.LogService;
import c5db.messages.generated.CommandReply;
import c5db.messages.generated.ModuleType;
import c5db.messages.generated.StartModule;
import c5db.messages.generated.StopModule;
import c5db.regionserver.RegionServerService;
import c5db.replication.ReplicatorService;
import c5db.tablet.TabletService;
import c5db.util.FiberOnly;
import com.dyuproject.protostuff.Message;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.channels.Request;
import org.jetlang.channels.RequestChannel;
import org.jetlang.core.Callback;
import org.jetlang.core.Disposable;
import org.jetlang.core.RunnableExecutorImpl;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;
import org.jetlang.fibers.ThreadFiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static c5db.log.OLog.moveAwayOldLogs;


/**
 * Holds information about all other modules, can start/stop other modules, etc.
 * Knows the 'root' information about this server as well, such as NodeId, etc.
 *
 * To shut down the 'server' module is to shut down the server.
 */
public class C5DB extends AbstractService implements C5Server {
    private static final Logger LOG = LoggerFactory.getLogger(C5DB.class);

    public static void main(String[] args) throws Exception {

        String username = System.getProperty("user.name");


        String cfgPath = "c5-" + System.currentTimeMillis();

        //  args = "nodeId", if none, then randomly generate i guess?

        if (args.length > 0) {
            cfgPath = args[0];
        }

        ConfigDirectory cfgDir = new ConfigDirectory(Paths.get("/tmp", username, cfgPath));

        // manually set the node id into the file.
        if (args.length > 1) {
            cfgDir.setNodeIdFile(args[1]);
        }
        if (args.length > 2) {
            cfgDir.setClusterNameFile(args[2]);
        }

        instance = new C5DB(cfgDir);
        instance.start();

        Random rnd = new Random();

        // issue startup commands here that are common/we always want:
        StartModule startLog = new StartModule(ModuleType.Log, 0, "");
        instance.getCommandChannel().publish(startLog);

        StartModule startBeacon = new StartModule(ModuleType.Discovery, 54333, "");
        instance.getCommandChannel().publish(startBeacon);


        StartModule startReplication = new StartModule(ModuleType.Replication, rnd.nextInt(30000) + 1024, "");
        instance.getCommandChannel().publish(startReplication);

        StartModule startTablet = new StartModule(ModuleType.Tablet, 0, "");
        instance.getCommandChannel().publish(startTablet);

        StartModule startRegionServer = new StartModule(ModuleType.RegionServer, 8080+rnd.nextInt(1000), "");
        instance.getCommandChannel().publish(startRegionServer);
    }

    private static C5Server instance = null;


    public C5DB(ConfigDirectory configDirectory) throws IOException {
        this.configDirectory = configDirectory;

        String data = configDirectory.getNodeId();
        long toNodeId = 0;
        if (data != null) {
            try {
                toNodeId = Long.parseLong(data);
            } catch (NumberFormatException ignored) {
            }
        }

        if (toNodeId == 0) {
            Random r = new Random();
            toNodeId = r.nextLong();
            configDirectory.setNodeIdFile(Long.toString(toNodeId));
        }

        this.nodeId = toNodeId;

//        String clusterNameData = configDirectory.getClusterName();
//        if (clusterNameData == null) {
//            clusterNameData = "the-cluster";
//            configDirectory.setClusterNameFile(clusterNameData);
//        }
//        this.clusterName = clusterNameData;
    }

    /**
     * Returns the server, but it will be null if you aren't running inside one.
     * @return
     */
    public static C5Server getServer() {
        return instance;
    }

    @Override
    public long getNodeId() {
        return nodeId;
    }

    @Override
    public ListenableFuture<C5Module> getModule(final ModuleType moduleType) {
        final SettableFuture<C5Module> future = SettableFuture.create();
        serverFiber.execute(new Runnable() {
            @Override
            public void run() {

                // What happens iff the moduleRegistry has EMPTY?
                if (!moduleRegistry.containsKey(moduleType)) {
                    // listen to the registration stream:
                    final Disposable[] d = new Disposable[]{null};
                    d[0] = getModuleStateChangeChannel().subscribe(serverFiber, new Callback<ModuleStateChange>() {
                        @Override
                        public void onMessage(ModuleStateChange message) {
                            if (message.state != State.RUNNING) return;

                            if (message.module.getModuleType().equals(moduleType)) {
                                future.set(message.module);

                                assert d[0] != null;  // this is pretty much impossible because of how fibers work.
                                d[0].dispose();
                            }
                        }
                    });
                }

                future.set(moduleRegistry.get(moduleType));
            }
        });
        return future;

    }

    @Override
    public ImmutableMap<ModuleType, C5Module> getModules() throws ExecutionException, InterruptedException {
        final SettableFuture<ImmutableMap<ModuleType, C5Module>> future = SettableFuture.create();
        serverFiber.execute(new Runnable() {
            @Override
            public void run() {
                future.set(ImmutableMap.copyOf(moduleRegistry));
            }
        });
        return future.get();
    }
    @Override
    public ListenableFuture<ImmutableMap<ModuleType, C5Module>> getModules2() {
        final SettableFuture<ImmutableMap<ModuleType, C5Module>> future = SettableFuture.create();
        serverFiber.execute(new Runnable() {
            @Override
            public void run() {
                future.set(ImmutableMap.copyOf(moduleRegistry));
            }
        });
        return future;
    }

    /**** Implementation ****/
    private Fiber tabletServicesFiber;

    private Fiber serverFiber;
    private final ConfigDirectory configDirectory;

    // The mapping between module name and the instance.
    private final Map<ModuleType, C5Module> moduleRegistry = new HashMap<>();

     private final long nodeId;

    private final Channel<Message<?>> commandChannel = new MemoryChannel<>();

    private PoolFiberFactory fiberPool;
    private NioEventLoopGroup bossGroup;
    private NioEventLoopGroup workerGroup;

    @Override
    public Channel<Message<?>> getCommandChannel() {
        return commandChannel;
    }

    public RequestChannel<Message<?>, CommandReply> commandRequests = new MemoryRequestChannel<>();
    @Override
    public RequestChannel<Message<?>, CommandReply> getCommandRequests() {
        return commandRequests;
    }

    private final Channel<ModuleStateChange> serviceRegisteredChannel = new MemoryChannel<>();
    @Override
    public Channel<ModuleStateChange> getModuleStateChangeChannel() {
        return serviceRegisteredChannel;
    }

    @Override
    public ConfigDirectory getConfigDirectory() {
        return configDirectory;
    }

    @Override
    public Channel<ConfigKeyUpdated> getConfigUpdateChannel() {

        // TODO this
        return null;
    }


    @FiberOnly
    private void processCommandMessage(Message<?> msg) throws Exception {
        if (msg instanceof StartModule) {
            StartModule message = (StartModule) msg;
            startModule(message.getModule(), message.getModulePort(), message.getModuleArgv());
        }
        else if (msg instanceof StopModule) {
            StopModule message = (StopModule)msg;

            stopModule(message.getModule(), message.getHardStop(), message.getStopReason());
        }
    }

    @FiberOnly
    private void processCommandRequest(Request<Message<?>, CommandReply> request) {
        Message<?> r = request.getRequest();
        try {
            String stdout = "";

            if (r instanceof StartModule) {
                StartModule message = (StartModule)r;
                startModule(message.getModule(), message.getModulePort(), message.getModuleArgv());

                stdout = String.format("Module %s started", message.getModule());
            } else if (r instanceof StopModule) {
                StopModule message = (StopModule)r;

                stopModule(message.getModule(), message.getHardStop(), message.getStopReason());

                stdout = String.format("Module %s started", message.getModule());
            } else {
                CommandReply reply = new CommandReply(false,
                        "",
                        String.format("Unknown message type: %s", r.getClass()));
                request.reply(reply);
                return;
            }

            CommandReply reply = new CommandReply(true, stdout, "");
            request.reply(reply);

        } catch (Exception e) {
            CommandReply reply = new CommandReply(false, "", e.toString());
            request.reply(reply);
        }
    }

    private class ModuleListenerPublisher implements Listener {
        private final C5Module module;

        private ModuleListenerPublisher(C5Module module) {
            this.module = module;
        }

        @Override
        public void starting() {
            LOG.debug("Starting module {}", module);
            publishEvent(State.STARTING);
        }

        @Override
        public void running() {
            LOG.debug("Running module {}", module);
            publishEvent(State.RUNNING);
        }

        @Override
        public void stopping(State from) {
            LOG.debug("Stopping module {}", module);
            publishEvent(State.STOPPING);
        }

        @Override
        public void terminated(State from) {
            // TODO move this into a subscriber of ourselves?
            LOG.debug("Terminated module {}", module);
            moduleRegistry.remove(module.getModuleType());
            publishEvent(State.TERMINATED);
        }

        @Override
        public void failed(State from, Throwable failure) {
            LOG.debug("Failed module {}", module);
            publishEvent(State.FAILED);
        }

        private void publishEvent(State state) {
            ModuleStateChange p = new ModuleStateChange(module, state);
            getModuleStateChangeChannel().publish(p);
        }

    }

    @FiberOnly
    private boolean startModule(final ModuleType moduleType, final int modulePort, String moduleArgv) throws Exception {
        if (moduleRegistry.containsKey(moduleType)) {
            // already running, dont start twice?
            LOG.warn("Module {} already running", moduleType);
            throw new Exception("Cant start, running, module: " + moduleType);
        }

        switch (moduleType) {
            case Discovery: {
                Map<ModuleType, Integer> l = new HashMap<>();
                for (ModuleType name : moduleRegistry.keySet()) {
                    l.put(name, moduleRegistry.get(name).port());
                }

                //C5Module module = new BeaconService(this.nodeId, modulePort, fiberPool.create(), workerGroup, l, this);
                //startServiceModule(module);
                break;
            }
            case Replication: {
                C5Module module = new ReplicatorService(fiberPool, bossGroup, workerGroup, modulePort, this);
                startServiceModule(module);
                break;
            }
            case Log: {
                C5Module module = new LogService(this);
                startServiceModule(module);

                break;
            }
            case Tablet: {
                C5Module module = new TabletService(fiberPool, this);
                startServiceModule(module);

                break;
            }
            case RegionServer: {
                C5Module module = new RegionServerService(fiberPool, bossGroup, workerGroup, modulePort, this);
                startServiceModule(module);

                break;
            }

            default:
                throw new Exception("No such module as " + moduleType);
        }

        return true;
    }

    private void startServiceModule(C5Module module) {
        LOG.info("Starting service {}", module.getModuleType());
        module.addListener(new ModuleListenerPublisher(module), serverFiber);

        module.start();
        moduleRegistry.put(module.getModuleType(), module);
    }

    @FiberOnly
    private void stopModule(ModuleType moduleType, boolean hardStop, String stopReason) {
        Service theModule = moduleRegistry.get(moduleType);
        if (theModule == null) {
            LOG.debug("Cant stop module {}, not in registry", moduleType);
            return ;
        }

        theModule.stop();
    }

    @Override
    protected void doStart() {
//        Path path;
//        path = Paths.get(getRandomPath());
//        RegistryFile registryFile;
        try {
//            registryFile = new RegistryFile(configDirectory.baseConfigPath);

            // TODO this should probably be done somewhere else.
            moveAwayOldLogs(configDirectory.baseConfigPath);

//            if (existingRegister(registryFile)) {
//                recoverC5Server(conf, path, registryFile);
//            } else {
//                bootStrapRegions(conf, path, registryFile);
//            }
        } catch (IOException e) {
            notifyFailed(e);
        }


        try {
            serverFiber = new ThreadFiber(new RunnableExecutorImpl(), "C5-Server", false);
            fiberPool = new PoolFiberFactory(Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()));
            bossGroup = new NioEventLoopGroup(1);
            workerGroup = new NioEventLoopGroup();

            commandChannel.subscribe(serverFiber, new Callback<Message<?>>() {
                @Override
                public void onMessage(Message<?> message) {
                    try {
                        processCommandMessage(message);
                    } catch (Exception e) {
                        LOG.warn("exception during message processing", e);
                    }
                }
            });

            commandRequests.subscribe(serverFiber, new Callback<Request<Message<?>, CommandReply>>() {
                @Override
                public void onMessage(Request<Message<?>, CommandReply> request) {
                    processCommandRequest(request);
                }
            });

            serverFiber.start();
            tabletServicesFiber.start();

            notifyStarted();
        } catch (Exception e) {
            notifyFailed(e);
        }
    }


    @Override
    protected void doStop() {
        // stop module set.

        // TODO write any last minute persistent data to disk (is there any?)
        // note: guava docs recommend doing long-acting operations in separate thread

        serverFiber.dispose();

        notifyStopped();
    }

}
