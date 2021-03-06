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

import c5db.AsyncChannelAsserts;
import c5db.interfaces.C5Server;
import c5db.interfaces.ReplicationModule;
import c5db.interfaces.replication.Replicator;
import c5db.interfaces.replication.ReplicatorInstanceEvent;
import c5db.interfaces.tablet.TabletStateChange;
import c5db.tablet.tabletCreationBehaviors.StartableTabletBehavior;
import c5db.util.ExceptionHandlingBatchExecutor;
import c5db.util.FiberSupplier;
import c5db.util.JUnitRuleFiberExceptions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.core.RunnableExecutorImpl;
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
import java.util.List;

import static c5db.AsyncChannelAsserts.assertEventually;
import static c5db.AsyncChannelAsserts.listenTo;
import static c5db.TabletMatchers.hasMessageWithState;

/**
 * TDD/unit test for tablet.
 */
public class ReplicatedTabletTest {
  @Rule
  public final JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(new Synchroniser());
  }};

  @Rule
  public final JUnitRuleFiberExceptions fiberExceptionRule = new JUnitRuleFiberExceptions();

  private MemoryChannel<Replicator.State> stateChannel;
  private MemoryChannel<ReplicatorInstanceEvent> replicatorEventChannel;

  final ReplicationModule replicationModule = context.mock(ReplicationModule.class);
  final Replicator replicator = context.mock(Replicator.class);
  final Region.Creator regionCreator = context.mock(Region.Creator.class);
  final Region region = context.mock(Region.class);
  final C5Server server = context.mock(C5Server.class);

  final SettableFuture<Replicator> future = SettableFuture.create();

  // Value objects for the test.
  final List<Long> peerList = ImmutableList.of(1L, 2L, 3L);
  final HRegionInfo regionInfo = new HRegionInfo(TableName.valueOf("tablename"));
  final String regionName = regionInfo.getRegionNameAsString();
  final HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("tablename"));
  private final StartableTabletBehavior userTabletLeaderBehavior = context.mock(StartableTabletBehavior.class);


  final Path path = Paths.get("/");
  final Configuration conf = new Configuration();

  private final FiberSupplier fiberSupplier = (ignore) ->
      new ThreadFiber(
          new RunnableExecutorImpl(new ExceptionHandlingBatchExecutor(fiberExceptionRule)),
          null,
          false);

  ReplicatedTablet replicatedTablet;

  AsyncChannelAsserts.ChannelListener<TabletStateChange> tabletStateChannelListener;

  @Before
  public void setup() throws Exception {
    context.checking(new Expectations() {{
      allowing(server).getFiberSupplier();
      will(returnValue(fiberSupplier));
    }});

    this.replicatedTablet = new ReplicatedTablet(server,
        regionInfo,
        tableDescriptor,
        peerList,
        path,
        conf,
        replicationModule,
        regionCreator,
        userTabletLeaderBehavior);
    tabletStateChannelListener = listenTo(replicatedTablet.getStateChangeChannel());

    future.set(replicator);
    tabletStateChannelListener = listenTo(replicatedTablet.getStateChangeChannel());
    stateChannel = new MemoryChannel<>();
    replicatorEventChannel = new MemoryChannel<>();

    context.checking(new Expectations() {
      {
        States state = context.states("start");

        allowing(replicator).getQuorumId();
        will(returnValue(regionName));

        oneOf(replicationModule).createReplicator(regionName, peerList);
        will(returnValue(future));
        then(state.is("opening"));

        oneOf(regionCreator).getHRegion(
            with(any(Path.class)),
            with(equal(regionInfo)),
            with(equal(tableDescriptor)),
            with(any(HLog.class)),
            with(same(conf)));
        will(returnValue(region));
        then(state.is("opened"));
        stateChannel = new MemoryChannel<>();
        replicatorEventChannel = new MemoryChannel<>();
      }
    });

    context.checking(new Expectations() {{
      allowing(replicator).getStateChannel();
      will(returnValue(stateChannel));

      allowing(replicator).getEventChannel();
      will(returnValue(replicatorEventChannel));

      allowing(replicator).getCommitNoticeChannel();

      allowing(replicator).getId();
    }});
  }

  @After
  public void after() {
    tabletStateChannelListener.dispose();
    replicatedTablet.dispose();
  }

  @Test
  public void basicTest() throws Throwable {
    replicatedTablet.start();
    assertEventually(tabletStateChannelListener, hasMessageWithState(c5db.interfaces.tablet.Tablet.State.Open));
  }

  @Test
  public void shouldPublishATabletStateChangeToLeaderWhenTheReplicatorBecomesTheLeader() throws Throwable {
    context.checking(new Expectations() {{
      // This behavior may or may not run before the test ends, and whether or not it runs is
      // not being tested here.
      allowing(userTabletLeaderBehavior).start();
    }});

    replicatedTablet.start();
    assertEventually(tabletStateChannelListener, hasMessageWithState(c5db.interfaces.tablet.Tablet.State.Open));

    stateChannel.publish(Replicator.State.LEADER);
    assertEventually(tabletStateChannelListener, hasMessageWithState(c5db.interfaces.tablet.Tablet.State.Leader));
  }
}