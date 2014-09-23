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
package c5db.tablet;

import c5db.ConfigDirectory;
import c5db.interfaces.C5Server;
import c5db.interfaces.ControlModule;
import c5db.interfaces.DiscoveryModule;
import c5db.interfaces.ReplicationModule;
import c5db.interfaces.discovery.NewNodeVisible;
import c5db.interfaces.tablet.Tablet;
import c5db.messages.generated.ModuleType;
import c5db.util.ExceptionHandlingBatchExecutor;
import c5db.util.FiberSupplier;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.fibers.PoolFiberFactory;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static c5db.FutureActions.returnFutureWithValue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class TabletServiceTest {
  Synchroniser sync = new Synchroniser();
  @Rule
  public final JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(sync);
  }};
  private final ExecutorService executorService = Executors.newSingleThreadExecutor();
  PoolFiberFactory poolFiberFactory = new PoolFiberFactory(executorService);

  DiscoveryModule discoveryModule = context.mock(DiscoveryModule.class);
  ControlModule controlModule = context.mock(ControlModule.class);
  ReplicationModule replicationModule = context.mock(ReplicationModule.class);
  ConfigDirectory configDirectory = context.mock(ConfigDirectory.class);

  C5Server c5Server = context.mock(C5Server.class);
  MemoryChannel<NewNodeVisible> nodeNotifications = new MemoryChannel<>();
  TabletService tabletService;

  private final FiberSupplier fiberSupplier = (throwableConsumer) ->
      poolFiberFactory.create(new ExceptionHandlingBatchExecutor(throwableConsumer));

  @Before
  public void before() throws Throwable {
    context.checking(new Expectations() {
      {
        allowing(c5Server).getFiberSupplier();
        will(returnValue(fiberSupplier));

        oneOf(c5Server).getModule(ModuleType.Discovery);
        will(returnFutureWithValue(discoveryModule));

        oneOf(c5Server).getModule(ModuleType.ControlRpc);
        will(returnFutureWithValue(controlModule));

        oneOf(c5Server).getModule(ModuleType.Replication);
        will(returnFutureWithValue(replicationModule));

        oneOf(c5Server).getConfigDirectory();
        will(returnValue(configDirectory));

        oneOf(discoveryModule).getNewNodeNotifications();
        will(returnValue(nodeNotifications));

      }
    });
    tabletService = new TabletService(c5Server);
    tabletService.start().get();
  }

  @After
  public void shutdownExecutorService() {
    poolFiberFactory.dispose();
    executorService.shutdownNow();
  }

  @Test
  public void shouldHaveAppropriateNumberOfTables() throws Throwable {
    Collection<Tablet> tablets = tabletService.getTablets();
    assertThat(tablets.size(), is(equalTo(0)));
  }
}