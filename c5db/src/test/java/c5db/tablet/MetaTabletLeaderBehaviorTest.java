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

import c5db.interfaces.C5Server;
import c5db.interfaces.ControlModule;
import c5db.interfaces.tablet.Tablet;
import c5db.messages.generated.ModuleType;
import com.google.common.util.concurrent.SettableFuture;
import org.jetlang.channels.Request;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

public class MetaTabletLeaderBehaviorTest {

  @Rule
  public final JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(new Synchroniser());
  }};
  private Tablet hRegionTablet;
  private C5Server c5Server;
  private ControlModule controlRpcModule;
  private SettableFuture<ControlModule> controlRpcFuture;

  @Before
  public void before() throws IOException {
    hRegionTablet = context.mock(Tablet.class, "mockHRegionTablet");
    c5Server = context.mock(C5Server.class, "mockC5Server");
    controlRpcFuture = SettableFuture.create();
    controlRpcModule = context.mock(ControlModule.class);
  }

  @Test
  public void shouldAddMyselfAsLeaderOfMetaToRoot() throws Throwable {
    context.checking(new Expectations() {{
      oneOf(hRegionTablet).getLeader();
      will(returnValue(1l));

      oneOf(c5Server).getModule(ModuleType.ControlRpc);
      will(returnValue(controlRpcFuture));

      oneOf(c5Server).getNodeId();
      will(returnValue(1l));

      oneOf(controlRpcModule).doMessage(with(any(Request.class)));
    }});
    MetaTabletLeaderBehavior metaTabletLeaderBehavior = new MetaTabletLeaderBehavior(hRegionTablet, c5Server);
    controlRpcFuture.set(controlRpcModule);
    metaTabletLeaderBehavior.start();
  }
}
