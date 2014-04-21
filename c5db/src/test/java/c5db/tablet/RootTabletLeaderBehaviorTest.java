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
import c5db.interfaces.C5Server;
import c5db.interfaces.TabletModule;
import c5db.messages.generated.ModuleSubCommand;
import c5db.messages.generated.ModuleType;
import io.protostuff.Message;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static c5db.AsyncChannelAsserts.assertEventually;
import static c5db.AsyncChannelAsserts.listenTo;

public class RootTabletLeaderBehaviorTest {

  @Rule
  public final JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(new Synchroniser());
  }};
  private TabletModule.Tablet hRegionTablet;
  private Region region;
  private C5Server c5Server;
  private Fiber fiber;
  private MemoryChannel<Message<?>> commandMemoryChannel = new MemoryChannel<>();
  AsyncChannelAsserts.ChannelListener commandListener;


  @After
  public void tearDown() {
    fiber.dispose();
  }

  @Before
  public void before() throws IOException {
    fiber = new ThreadFiber();

    hRegionTablet = context.mock(TabletModule.Tablet.class, "mockHRegionTablet");
    region = context.mock(Region.class, "mockRegion");
    c5Server = context.mock(C5Server.class, "mockC5Server");
  }

  @Test
  public void shouldBootStrapMetaOnlyWhenRootIsBlank() throws Throwable {
    List<Long> fakePeers = Arrays.asList(0l);
    context.checking(new Expectations() {{
      oneOf(hRegionTablet).getRegion();
      will(returnValue(region));

      oneOf(region).get(with(any(Get.class)));
      will(returnValue(Result.create(new Cell[]{})));

      oneOf(hRegionTablet).getPeers();
      will(returnValue(fakePeers));

      exactly(2).of(c5Server).isSingleNodeMode();
      will(returnValue(true));

      oneOf(region).put(with(any(Put.class)));

      // Post put we send a command over the command channel
      exactly(2).of(c5Server).getCommandChannel();
      will(returnValue(commandMemoryChannel));

    }});

    commandListener = listenTo(c5Server.getCommandChannel());
    RootTabletLeaderBehavior rootTabletLeaderBehavior = new RootTabletLeaderBehavior(hRegionTablet,
        c5Server);
    rootTabletLeaderBehavior.start();
    assertEventually(commandListener, hasMessageWithRPC(C5ServerConstants.START_META));
  }

  @Test
  public void shouldSkipBootStrapMetaOnlyWhenRootIsNotBlank() throws Throwable {
    Cell bonkCell = new KeyValue(Bytes.toBytes("123"), 0l);
    Result results = Result.create(new Cell[]{bonkCell});

    context.checking(new Expectations() {{
      oneOf(hRegionTablet).getRegion();
      will(returnValue(region));

      oneOf(region).get(with(any(Get.class)));
      will(returnValue(results));

      never(hRegionTablet).getPeers();
      never(c5Server).isSingleNodeMode();
      never(region).put(with(any(Put.class)));

    }});

    RootTabletLeaderBehavior rootTabletLeaderBehavior = new RootTabletLeaderBehavior(hRegionTablet, c5Server);
    rootTabletLeaderBehavior.start();

  }


  private Matcher<ModuleSubCommand> hasMessageWithRPC(String s) {
    return new MessageMatcher(s);
  }

  private class MessageMatcher extends TypeSafeMatcher<ModuleSubCommand> {
    private final String s;

    public MessageMatcher(String s) {
      this.s = s;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("a command which is: ").appendValue(s);
    }

    @Override
    protected boolean matchesSafely(ModuleSubCommand message) {
      return message.getModule().equals(ModuleType.Tablet) &&
          message.getSubCommand().equals(C5ServerConstants.START_META);
    }
  }
}
