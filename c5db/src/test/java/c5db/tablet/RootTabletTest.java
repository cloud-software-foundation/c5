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

import c5db.AsyncChannelAsserts;
import c5db.interfaces.C5Server;
import c5db.interfaces.ReplicationModule;
import c5db.interfaces.replication.Replicator;
import c5db.interfaces.tablet.TabletStateChange;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import io.protostuff.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
import org.jmock.Expectations;
import org.jmock.States;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static c5db.AsyncChannelAsserts.assertEventually;
import static c5db.AsyncChannelAsserts.listenTo;
import static c5db.TabletMatchers.hasMessageWithState;

/**
 * TDD/unit test for tablet.
 */
public class RootTabletTest {
  @Rule
  public final JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(new Synchroniser());
  }};

  private MemoryChannel<Message<?>> commandMemoryChannel;

  private MemoryChannel<Replicator.State> stateMemoryChannel;

  final ReplicationModule replicationModule = context.mock(ReplicationModule.class);
  final Replicator replicator = context.mock(Replicator.class);
  final Region.Creator regionCreator = context.mock(Region.Creator.class);
  final Region region = context.mock(Region.class);
  final C5Server server = context.mock(C5Server.class);
  final SettableFuture<Replicator> future = SettableFuture.create();

  // Value objects for the test.
  final List<Long> peerList = ImmutableList.of(1L, 2L, 3L);
  final HRegionInfo regionInfo = new HRegionInfo(TableName.valueOf("hbase", "root"));
  final String regionName = regionInfo.getRegionNameAsString();
  final HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("hbase", "root"));

  final Path path = Paths.get("/");
  final Configuration conf = new Configuration();

  final Fiber tabletFiber = new ThreadFiber();
  ReplicatedTablet replicatedTablet = new ReplicatedTablet(server,
      regionInfo,
      tableDescriptor,
      peerList,
      path,
      conf,
      tabletFiber,
      replicationModule,
      regionCreator);

  AsyncChannelAsserts.ChannelListener<TabletStateChange> stateChangeChannelListener;

  @Before
  public void setup() throws Exception {
    Fiber tabletFiber = new ThreadFiber();
    this.replicatedTablet = new ReplicatedTablet(server,
        regionInfo,
        tableDescriptor,
        peerList,
        path,
        conf,
        tabletFiber,
        replicationModule,
        regionCreator);
    future.set(replicator);
    stateChangeChannelListener = listenTo(replicatedTablet.getStateChangeChannel());
    stateMemoryChannel = new MemoryChannel<>();
    commandMemoryChannel = new MemoryChannel<>();

    context.checking(new Expectations() {
      {
        States state = context.states("start");

        allowing(replicator).getQuorumId();
        will(returnValue(regionName));

        oneOf(replicationModule).createReplicator(regionName, peerList);
        will(returnValue(future));
        then(state.is("opening"));

        oneOf(replicator).start();
        when(state.is("opening"));

        oneOf(regionCreator).getHRegion(
            with(any(Path.class)),
            with(equal(regionInfo)),
            with(equal(tableDescriptor)),
            with(any(HLog.class)),
            with(same(conf)));
        will(returnValue(region));
        then(state.is("opened"));

        // Return 0 entries from the root table for Meta
        oneOf(region).get(with(any(Get.class)));
        will(returnValue(org.apache.hadoop.hbase.client.Result.create(new ArrayList<>())));

        oneOf(server).isSingleNodeMode();
        will(returnValue(true));

        // Return 0 entries from the root table for Meta
        oneOf(region).put(with(any(Put.class)));

        // Post put we send a command over the command channel
        oneOf(server).getCommandChannel();
        will(returnValue(commandMemoryChannel));

        allowing(replicator).getStateChannel();
        will(returnValue(stateMemoryChannel));
      }
    });
  }

  @After
  public void after() {
    tabletFiber.dispose();
    stateChangeChannelListener.dispose();
  }

  @Test
  public void shouldRunCallCallbackWhenTabletBecomesTheLeader() throws Throwable {
    replicatedTablet.start();
    assertEventually(stateChangeChannelListener, hasMessageWithState(c5db.interfaces.tablet.Tablet.State.Open));
    stateMemoryChannel.publish(Replicator.State.LEADER);
    assertEventually(stateChangeChannelListener, hasMessageWithState(c5db.interfaces.tablet.Tablet.State.Leader));
    Thread.sleep(10000);
  }
}