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

import c5db.C5Compare;
import c5db.TestHelpers;
import c5db.client.generated.Condition;
import c5db.client.generated.MutationProto;
import c5db.client.generated.TableName;
import c5db.interfaces.tablet.Tablet;
import c5db.util.TabletNameHelpers;
import org.apache.hadoop.hbase.HRegionInfo;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.jetlang.channels.Request;
import org.jmock.Expectations;
import org.jmock.States;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.Arrays;
import java.util.concurrent.ConcurrentSkipListMap;

public class BasicTableCreationTest extends TabletServiceTest {

  @Rule
  public TestName name = new TestName();

  private final Tablet tablet = context.mock(Tablet.class);
  private final Region region = context.mock(Region.class);

  @Test
  public void shouldBeAbleToStartServer() throws Throwable {
    final States searching = context.states("running");

    TableName clientTableName = TabletNameHelpers.getClientTableName("c5", name.getMethodName());
    org.apache.hadoop.hbase.TableName tableName = TabletNameHelpers.getHBaseTableName(clientTableName);

    HRegionInfo hRegionInfo = new HRegionInfo(tableName, new byte[]{}, new byte[]{});
    context.checking(new Expectations() {
      {
        allowing(c5Server).getNodeId();
        will(returnValue(1l));

        oneOf(controlModule).doMessage(with(any(Request.class)));
        then(searching.is("done")); //TODO make sure the message is correct

        allowing(tablet).getRegionInfo();
        will(returnValue(hRegionInfo));

        oneOf(tablet).getLeader();
        will(returnValue(1l));

        oneOf(tablet).getRegion();
        will(returnValue(region));

        oneOf(region).mutate(with(any(MutationProto.class)), with(any(Condition.class)));
        will(returnValue(true));
      }
    });
    NonBlockingHashMap<String, ConcurrentSkipListMap<byte[], Tablet>> tabletRegistryTables
        = tabletService.tabletRegistry.getTables();
    ConcurrentSkipListMap<byte[], Tablet> metaTablet = new ConcurrentSkipListMap<>(new C5Compare());
    metaTablet.put(new byte[]{0x00}, tablet);
    tabletRegistryTables.put("hbase:meta", metaTablet);
    tabletService.acceptCommand(TestHelpers.getCreateTabletSubCommand(tableName,
        new byte[][]{},
        Arrays.asList(c5Server)));
    sync.waitUntil(searching.is("done"));
  }
}