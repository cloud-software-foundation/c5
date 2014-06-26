package c5db.tablet;

import c5db.client.generated.Scan;
import c5db.interfaces.C5Server;
import c5db.interfaces.ControlModule;
import c5db.interfaces.TabletModule;
import c5db.interfaces.tablet.Tablet;
import c5db.messages.generated.ModuleType;
import c5db.regionserver.AddElementsActionReturnTrue;
import c5db.tablet.tabletCreationBehaviors.UserTabletLeaderBehavior;
import c5db.util.TabletNameHelpers;
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
import java.util.ArrayList;
import java.util.List;

import static c5db.FutureActions.returnFutureWithValue;

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
    ArrayList<KeyValue> kvs = new ArrayList<>();
    byte[] cf = Bytes.toBytes("cf");
    byte[] value = Bytes.toBytes("value");
    Integer count = 3;
    for (int i = 0; i != count; i++) {
      kvs.add(new KeyValue(Bytes.toBytes("row"), cf, Bytes.toBytes(i), value));
    }

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

      oneOf(rootRegionScanner).next(with(any(List.class)));
      will(AddElementsActionReturnTrue.addElements(kvs.toArray()));

      oneOf(tablet).getRegionInfo();
      will(returnValue(userRegionInfo));
    }});

    UserTabletLeaderBehavior userTabletLeaderBehavior = new UserTabletLeaderBehavior(tablet, c5Server);
    userTabletLeaderBehavior.start();
    synchronizer.waitUntil(searching.is("done"));
  }
}