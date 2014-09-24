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

import c5db.C5ServerConstants;
import c5db.client.generated.Scan;
import c5db.interfaces.ControlModule;
import c5db.interfaces.ModuleInformationProvider;
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
  private final org.apache.hadoop.hbase.TableName tableName =
      TabletNameHelpers.getHBaseTableName(TabletNameHelpers.getClientTableName("c5", "userTable"));
  private final HRegionInfo userRegionInfo = new HRegionInfo(tableName, new byte[0], new byte[0]);
  private final Synchroniser synchroniser = new Synchroniser();
  @Rule
  public final JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(synchroniser);
  }};

  private final ModuleInformationProvider moduleInformationProvider = context.mock(ModuleInformationProvider.class);
  private final ControlModule controlRpcModule = context.mock(ControlModule.class);
  private final TabletModule tabletModule = context.mock(TabletModule.class);
  private final Tablet rootTablet = context.mock(Tablet.class, "rootTablet");
  private final Region rootRegion = context.mock(Region.class, "rootRegion ");
  private final RegionScanner rootRegionScanner = context.mock(RegionScanner.class, "rrs");
  private final States searching = context.states("running");
  private final Tablet tablet = context.mock(Tablet.class, "userTablet");


  @Test
  public void shouldAddMyselfAsLeaderOfMetaToRoot() throws Throwable {
    context.checking(new Expectations() {{
      oneOf(moduleInformationProvider).getModule(ModuleType.ControlRpc);
      will(returnFutureWithValue(controlRpcModule));

      oneOf(controlRpcModule).doMessage(with(any(Request.class)));
      then(searching.is("done"));//TODO make sure the message is correct

      // Prepare to get the root region
      oneOf(moduleInformationProvider).getModule(ModuleType.Tablet);
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
      will(addElements(aKeyValue()));
    }});

    UserTabletLeaderBehavior userTabletLeaderBehavior =
        new UserTabletLeaderBehavior(moduleInformationProvider, userRegionInfo);
    userTabletLeaderBehavior.start();
    synchroniser.waitUntil(searching.is("done"));
  }

  private KeyValue aKeyValue() {
    byte[] rootRow = Bytes.toBytes("row");
    byte[] rootCf = HConstants.CATALOG_FAMILY;
    byte[] rootCq = C5ServerConstants.LEADER_QUALIFIER;
    byte[] rootValue = Bytes.toBytes(1l);
    return new KeyValue(rootRow, rootCf, rootCq, rootValue);
  }
}
