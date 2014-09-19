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

import c5db.C5ServerConstants;
import c5db.client.generated.Scan;
import c5db.interfaces.C5Server;
import c5db.interfaces.ControlModule;
import c5db.interfaces.TabletModule;
import c5db.interfaces.tablet.Tablet;
import c5db.messages.generated.ModuleType;
import c5db.tablet.tabletCreationBehaviors.UserTabletLeaderBehavior;
import c5db.util.TabletNameHelpers;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetlang.channels.Request;
import org.jmock.Expectations;
import org.jmock.States;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Rule;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static c5db.FutureActions.returnFutureWithValue;
import static c5db.regionserver.AddElementsActionReturnTrue.addElements;

public class UserTabletLeaderBehaviorTest {
  org.apache.hadoop.hbase.TableName tableName =
      TabletNameHelpers.getHBaseTableName(TabletNameHelpers.getClientTableName("c5", "userTable"));
  private final HRegionInfo userRegionInfo = new HRegionInfo(tableName, new byte[0], new byte[0]);
  Synchroniser synchronizer = new Synchroniser();
  @Rule
  public final JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(synchronizer);
  }};

  private C5Server c5Server = context.mock(C5Server.class);
  private ControlModule controlRpcModule = context.mock(ControlModule.class);
  private TabletModule tabletModule = context.mock(TabletModule.class);
  private Tablet rootTablet = context.mock(Tablet.class, "rootTablet");
  private Region rootRegion = context.mock(Region.class, "rootRegion ");
  private RegionScanner rootRegionScanner = context.mock(RegionScanner.class, "rrs");
  final States searching = context.states("running");
  private Tablet tablet = context.mock(Tablet.class, "userTablet");


  @Test
  public void shouldAddMyselfAsLeaderOfMetaToRoot() throws Throwable {
    byte[] rootRow = Bytes.toBytes("row");
    byte[] rootCf = HConstants.CATALOG_FAMILY;
    byte[] rootCq = C5ServerConstants.LEADER_QUALIFIER;
    byte[] rootValue = Bytes.toBytes(1l);
    KeyValue rootKV = new KeyValue(rootRow, rootCf, rootCq, rootValue);

    context.checking(new Expectations() {{
      oneOf(c5Server).getModule(ModuleType.ControlRpc);
      will(returnFutureWithValue(controlRpcModule));

      oneOf(controlRpcModule).doMessage(with(any(Request.class)));
      then(searching.is("done"));//TODO make sure the message is correct

      // Prepare to get the root region
      oneOf(c5Server).getModule(ModuleType.Tablet);
      will(returnFutureWithValue(tabletModule));

      oneOf(tabletModule).getTablet(with(any(String.class)), with(any(ByteBuffer.class)));
      will(returnValue(rootTablet));

      oneOf(rootTablet).getRegion();
      will(returnValue(rootRegion));

      oneOf(rootRegion).getScanner(with(any(Scan.class)));
      will(returnValue(rootRegionScanner));

      allowing(tablet).getRegionInfo();
      will(returnValue(userRegionInfo));

      oneOf(rootRegionScanner).nextRaw(with(any(List.class)));
      will(addElements(rootKV));
    }});

    UserTabletLeaderBehavior userTabletLeaderBehavior = new UserTabletLeaderBehavior(c5Server, userRegionInfo);
    userTabletLeaderBehavior.start();
    synchronizer.waitUntil(searching.is("done"));
  }
}
