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
import c5db.discovery.generated.Availability;
import c5db.interfaces.discovery.NewNodeVisible;
import c5db.interfaces.discovery.NodeInfo;
import c5db.interfaces.replication.Replicator;
import c5db.interfaces.replication.ReplicatorReceipt;
import c5db.interfaces.tablet.Tablet;
import c5db.interfaces.tablet.TabletStateChange;
import c5db.messages.generated.ModuleType;

import com.google.common.collect.ImmutableMap;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.fibers.Fiber;
import org.jmock.Expectations;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static c5db.AsyncChannelAsserts.assertEventually;
import static c5db.AsyncChannelAsserts.listenTo;
import static c5db.FutureActions.returnFutureWithValue;
import static c5db.TabletMatchers.hasMessageWithState;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class StartedRootOnTabletServiceTest extends TabletServiceTest {

  Replicator replicator = context.mock(Replicator.class);
  Channel<Replicator.State> stateChannel = new MemoryChannel<>();
  Channel<Tablet.State> stateChangeChannel = new MemoryChannel<>();
  Fiber fiber = poolFiberFactory.create();

  @Before
  public void startRoot() throws Throwable {
    Map<Long, NodeInfo> discoveryMap = new HashMap<>();
    discoveryMap.put(1l, new NodeInfo(new Availability()));
    context.checking(new Expectations() {
      {
        oneOf(configDirectory).getBaseConfigPath();
        will(returnValue(Files.createTempDirectory(null)));

        allowing(c5Server).isSingleNodeMode();
        will(returnValue(true));

        oneOf(discoveryModule).getState();
        will(returnFutureWithValue(ImmutableMap.copyOf(discoveryMap)));

        oneOf(replicationModule).createReplicator(with(any(String.class)), with(any(List.class)));
        will(returnFutureWithValue(replicator));

        oneOf(replicator).getStateChannel();
        will(returnValue(stateChannel));

        allowing(replicator).getEventChannel();
        will(returnValue(stateChangeChannel));

        allowing(configDirectory).writeBinaryData(with(any(String.class)), with(any(String.class)), with(any(byte[].class)));
        allowing(configDirectory).writePeersToFile(with(any(String.class)), with(any(List.class)));

        oneOf(c5FiberFactory).create();
        will(returnValue(fiber));

        oneOf(replicator).start();

        oneOf(replicator).getQuorumId();
        will(returnValue("hbase:root,,1.9e44d7942d3598d55c758b7b83373c71."));

        allowing(replicator).getId();
        oneOf(replicator).getCommitNoticeChannel();

        allowing(replicator).logData(with(any(List.class)));
        will(returnFutureWithValue(new ReplicatorReceipt(102l, 2l)));

        allowing(c5Server).getModule(ModuleType.ControlRpc);
        will(returnFutureWithValue(controlModule));

      }
    });
    nodeNotifications.publish(new NewNodeVisible(1l, new NodeInfo(new Availability())));
    AsyncChannelAsserts.ChannelListener<TabletStateChange> tabletStateListener
        = listenTo(tabletService.getTabletStateChanges());

    assertEventually(tabletStateListener, hasMessageWithState(Tablet.State.CreatingReplicator));
    assertEventually(tabletStateListener, hasMessageWithState(Tablet.State.Open));

    stateChannel.publish(Replicator.State.LEADER);
    assertEventually(tabletStateListener, hasMessageWithState(Tablet.State.Leader));
  }

  @Test
  public void shouldHaveAppropriateNumberOfTables() throws Throwable {
    Collection<Tablet> tablets = tabletService.getTablets();
    assertThat(tablets.size(), is(equalTo(1)));
  }
}