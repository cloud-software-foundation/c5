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

package c5db.replication;

import c5db.C5CommonTestUtil;
import c5db.ConfigDirectory;
import c5db.NioFileConfigDirectory;
import c5db.discovery.BeaconService;
import c5db.interfaces.C5Module;
import c5db.interfaces.DiscoveryModule;
import c5db.interfaces.LogModule;
import c5db.interfaces.ModuleServer;
import c5db.interfaces.ReplicationModule;
import c5db.interfaces.replication.Replicator;
import c5db.log.LogService;
import c5db.log.LogTestUtil;
import c5db.messages.generated.ModuleType;
import c5db.util.ExceptionHandlingBatchExecutor;
import c5db.util.JUnitRuleFiberExceptions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.Subscriber;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static c5db.C5ServerConstants.DISCOVERY_PORT;
import static c5db.C5ServerConstants.REPLICATOR_PORT_MIN;
import static c5db.replication.ReplicatorService.FiberFactory;

public class GeneralReplicatorTest {
  @Rule
  public JUnitRuleFiberExceptions jUnitFiberExceptionHandler = new JUnitRuleFiberExceptions();

  private static final int NUMBER_OF_PROCESSORS = Runtime.getRuntime().availableProcessors();

  private final ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_PROCESSORS);
  private final EventLoopGroup bossGroup = new NioEventLoopGroup(NUMBER_OF_PROCESSORS / 3);
  private final EventLoopGroup workerGroup = new NioEventLoopGroup(NUMBER_OF_PROCESSORS / 3);

  private final PoolFiberFactory fiberFactory = new PoolFiberFactory(executorService);
  private final Set<Fiber> fibers = new HashSet<>();

  private ConfigDirectory configDirectory;

  @Before
  public void setupConfigDirectory() throws Exception {
    configDirectory = new NioFileConfigDirectory(new C5CommonTestUtil().getDataTestDir("general-replicator-test"));
  }

  @After
  public void disposeOfResources() throws Exception {
    fiberFactory.dispose();
    executorService.shutdownNow();
    fibers.forEach(Fiber::dispose);

    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();

    bossGroup.terminationFuture().get();
    workerGroup.terminationFuture().get();
  }

  @Test(timeout = 3000)
  public void establishesASystemForGenericReplicationOfDataAcrossNodes() throws Exception {
    ReplicationServer replicationServer = new ReplicationServer(1, REPLICATOR_PORT_MIN, DISCOVERY_PORT, this::newTestFiber);
    replicationServer.start();

    Replicator replicator = replicationServer.createReplicator("quorumId", Lists.newArrayList(1L)).get();

    replicator.logData(Lists.newArrayList(LogTestUtil.someData())).get();
  }

  private Fiber newTestFiber(Consumer<Throwable> throwableHandler) {
    Fiber newFiber = fiberFactory.create(new ExceptionHandlingBatchExecutor(throwableHandler));
    fibers.add(newFiber);
    return newFiber;
  }

  private class SynchronousModuleServer implements ModuleServer {
    private final Map<ModuleType, C5Module> modules = new HashMap<>();
    private final Map<ModuleType, Integer> modulePorts = new HashMap<>();
    private final Channel<ImmutableMap<ModuleType, Integer>> modulePortsChannel = new MemoryChannel<>();

    public void addAndStartModule(C5Module module) throws InterruptedException, ExecutionException {
      module.start().get();

      modules.put(module.getModuleType(), module);
      modulePorts.put(module.getModuleType(), module.port());
      modulePortsChannel.publish(ImmutableMap.copyOf(modulePorts));
    }

    @Override
    public ListenableFuture<C5Module> getModule(ModuleType moduleType) {
      return Futures.immediateFuture(modules.get(moduleType));
    }

    @Override
    public Subscriber<ImmutableMap<ModuleType, Integer>> availableModulePortsChannel() {
      return modulePortsChannel;
    }

    @Override
    public ImmutableMap<ModuleType, C5Module> getModules()
        throws ExecutionException, InterruptedException, TimeoutException {
      throw new UnsupportedOperationException();
    }
  }

  private class ReplicationServer implements AutoCloseable {
    private final SynchronousModuleServer moduleServer = new SynchronousModuleServer();
    private final Fiber discoveryFiber;
    private final DiscoveryModule discoveryModule;
    private final LogModule logModule = new LogService(configDirectory);
    private final ReplicationModule replicationModule;

    public ReplicationServer(long nodeId, int replicatorPort, int discoveryPort, FiberFactory fiberFactory) {
      // BeaconService will start this Fiber on its own
      discoveryFiber = fiberFactory.getFiber(jUnitFiberExceptionHandler);
      discoveryModule = new BeaconService(nodeId, discoveryPort, discoveryFiber, workerGroup, ImmutableMap.of(), moduleServer);
      replicationModule = new ReplicatorService(bossGroup, workerGroup, nodeId, replicatorPort, moduleServer, fiberFactory, configDirectory);
    }

    public void start() throws InterruptedException, ExecutionException {
      moduleServer.addAndStartModule(logModule);
      moduleServer.addAndStartModule(discoveryModule);
      moduleServer.addAndStartModule(replicationModule);
    }

    public ListenableFuture<Replicator> createReplicator(String quorumId, Collection<Long> peerIds) {
      return replicationModule.createReplicator(quorumId, peerIds);
    }

    @Override
    public void close() {
      replicationModule.stopAndWait();
      discoveryModule.stopAndWait();
      logModule.stopAndWait();

      discoveryFiber.dispose();
    }
  }

}
