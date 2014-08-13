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
import c5db.C5ServerConstants;
import c5db.client.generated.Condition;
import c5db.client.generated.Get;
import c5db.client.generated.MutationProto;
import c5db.interfaces.C5Server;
import c5db.interfaces.ReplicationModule;
import c5db.interfaces.replication.Replicator;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.interfaces.tablet.TabletStateChange;
import c5db.messages.generated.ModuleSubCommand;
import c5db.util.ExceptionHandlingBatchExecutor;
import c5db.util.FiberSupplier;
import c5db.util.JUnitRuleFiberExceptions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import io.protostuff.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.jetlang.channels.Channel;
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
public class RootTabletTest {
  @Rule
  public final JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(new Synchroniser());
  }};

  @Rule
  public final JUnitRuleFiberExceptions fiberExceptionRule = new JUnitRuleFiberExceptions();

  private Channel<Message<?>> commandMemoryChannel;

  private MemoryChannel<Replicator.State> stateMemoryChannel;

  private final ReplicationModule replicationModule = context.mock(ReplicationModule.class);
  private final Replicator replicator = context.mock(Replicator.class);
  private final Region.Creator regionCreator = context.mock(Region.Creator.class);
  private final Region region = context.mock(Region.class);
  private final C5Server server = context.mock(C5Server.class);
  private final SettableFuture<Replicator> future = SettableFuture.create();

  // Value objects for the test.
  private final List<Long> peerList = ImmutableList.of(1L, 2L, 3L);
  private final HRegionInfo regionInfo = new HRegionInfo(TableName.valueOf("hbase", "root"));
  private final String regionName = regionInfo.getRegionNameAsString();
  private final HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("hbase", "root"));

  private final Path path = Paths.get("/");
  private final Configuration conf = new Configuration();

  private final FiberSupplier fiberSupplier = (ignore) ->
      new ThreadFiber(
          new RunnableExecutorImpl(new ExceptionHandlingBatchExecutor(fiberExceptionRule)),
          null,
          false);

  private ReplicatedTablet replicatedTablet;

  private AsyncChannelAsserts.ChannelListener<TabletStateChange> stateChangeChannelListener;

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
        regionCreator);
    future.set(replicator);
    stateChangeChannelListener = listenTo(replicatedTablet.getStateChangeChannel());
    stateMemoryChannel = new MemoryChannel<>();
    commandMemoryChannel = new MemoryChannel<>();

    context.checking(new Expectations() {
      {
        allowing(replicator).getQuorumId();
        will(returnValue(regionName));

        allowing(replicator).getStateChannel();
        will(returnValue(stateMemoryChannel));

        allowing(replicator).getEventChannel();
        will(returnValue(new MemoryChannel<>()));

        allowing(replicator).getCommitNoticeChannel();

        allowing(replicator).getId();
      }
    });
  }

  @After
  public void after() {
    stateChangeChannelListener.dispose();
    replicatedTablet.dispose();
  }

  @Test
  public void shouldRunCallCallbackWhenTabletBecomesTheLeader() throws Throwable {
    States state = context.states("start");

    context.checking(new Expectations() {
      {
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

      }
    });
    replicatedTablet.start();
    assertEventually(stateChangeChannelListener, hasMessageWithState(c5db.interfaces.tablet.Tablet.State.Open));

    context.checking(new Expectations() {
      {
        // Return 0 entries from the root table for Meta
        oneOf(region).exists(with(any(Get.class)));
        will(returnValue(false));

        oneOf(server).isSingleNodeMode();
        will(returnValue(true));

        allowing(region).mutate(with(any(MutationProto.class)), with(any(Condition.class)));
        will(returnValue(true));

        // Post put we send a command over the command commandRpcRequestChannel
        oneOf(server).getCommandChannel();
        will(returnValue(commandMemoryChannel));
      }
    });

    AsyncChannelAsserts.ChannelListener<Message<?>> commandListener = listenTo(commandMemoryChannel);
    stateMemoryChannel.publish(Replicator.State.LEADER);
    assertEventually(stateChangeChannelListener, hasMessageWithState(c5db.interfaces.tablet.Tablet.State.Leader));

    assertEventually(commandListener, hasSubmoduleWithCommand(C5ServerConstants.START_META));

  }

  public static Matcher<Message> hasSubmoduleWithCommand(String command) {
    return new CommandMatcher<Message>(command);
  }

  private static class CommandMatcher<T> extends BaseMatcher<Message> {
    private final String command;

    public CommandMatcher(String command) {
      this.command = command;
    }

    @Override
    public boolean matches(Object o) {
      CommandRpcRequest commandRpcRequest = (CommandRpcRequest) o;
      ModuleSubCommand moduleSubCommand = (ModuleSubCommand) commandRpcRequest.message;
      return moduleSubCommand.getSubCommand().startsWith(command);
    }

    @Override
    public void describeTo(Description description) {

    }
  }
}