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

import c5db.interfaces.C5Server;
import c5db.interfaces.ControlModule;
import c5db.interfaces.TabletModule;
import c5db.interfaces.tablet.Tablet;
import c5db.messages.generated.ModuleType;
import c5db.tablet.tabletCreationBehaviors.MetaTabletLeaderBehavior;
import org.jetlang.channels.Request;
import org.jmock.Expectations;
import org.jmock.States;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static c5db.FutureActions.returnFutureWithValue;

public class MetaTabletLeaderBehaviorTest {
  private final Synchroniser sync = new Synchroniser();
  @Rule
  public final JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(sync);
  }};

  private Tablet rootTablet;
  private C5Server c5Server;
  private ControlModule controlRpcModule;
  private TabletModule tabletModule;

  @Before
  public void before() throws IOException {
    rootTablet = context.mock(Tablet.class, "rootTablet");
    c5Server = context.mock(C5Server.class, "mockC5Server");
    controlRpcModule = context.mock(ControlModule.class);
    tabletModule = context.mock(TabletModule.class);
  }

  @Test
  public void shouldAddMyselfAsLeaderOfMetaToRoot() throws Throwable {
    final States state = context.states("request-message").startsAs("not-run");
    context.checking(new Expectations() {{
      oneOf(c5Server).getModule(ModuleType.ControlRpc);
      will(returnFutureWithValue(controlRpcModule));

      oneOf(c5Server).getModule(ModuleType.Tablet);
      will(returnFutureWithValue(tabletModule));

      oneOf(tabletModule).getTablet(with(any(String.class)), with(any(ByteBuffer.class)));
      will(returnValue(rootTablet));

      oneOf(rootTablet).getLeader();
      will(returnValue(1l));

      oneOf(c5Server).getNodeId();
      will(returnValue(1l));

      oneOf(controlRpcModule).doMessage(with(any(Request.class)));
      then(state.is("done"));
    }});
    MetaTabletLeaderBehavior metaTabletLeaderBehavior = new MetaTabletLeaderBehavior(c5Server);
    metaTabletLeaderBehavior.start();
    sync.waitUntil(state.is("done"));
  }
}