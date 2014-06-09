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

package c5db.client;


import c5db.client.generated.Call;
import c5db.client.generated.Cell;
import c5db.client.generated.CellType;
import c5db.client.generated.Condition;
import c5db.client.generated.Get;
import c5db.client.generated.GetRequest;
import c5db.client.generated.GetResponse;
import c5db.client.generated.MultiRequest;
import c5db.client.generated.MultiResponse;
import c5db.client.generated.MutateRequest;
import c5db.client.generated.MutateResponse;
import c5db.client.generated.MutationProto;
import c5db.client.generated.RegionAction;
import c5db.client.generated.RegionActionResult;
import c5db.client.generated.RegionSpecifier;
import c5db.client.generated.Response;
import c5db.client.generated.Result;
import c5db.client.generated.Scan;
import c5db.client.generated.ScanRequest;
import c5db.client.generated.ScanResponse;
import c5db.client.scanner.C5ClientScanner;
import c5db.client.scanner.C5QueueBasedClientScanner;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import org.apache.hadoop.hbase.util.Bytes;
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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static c5db.FutureActions.returnFutureWithValue;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class C5DatabaseTest {
  @Rule
  public JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(new Synchroniser());
  }};

  private final MessageHandler messageHandler = context.mock(MessageHandler.class);
  private final ChannelPipeline channelPipeline = context.mock(ChannelPipeline.class);
  private final C5ConnectionManager c5ConnectionManager = context.mock(C5ConnectionManager.class);
  private final Channel channel = context.mock(Channel.class);

  private ExplicitNodeCaller singleNodeTableInterface;

  @Before
  public void before() throws InterruptedException, ExecutionException, TimeoutException {
    context.checking(new Expectations() {
      {
        oneOf(c5ConnectionManager).getOrCreateChannel(with(any(String.class)), with(any(int.class)));
        will(returnValue(channel));

        oneOf(channel).pipeline();
        will(returnValue(channelPipeline));

        oneOf(channelPipeline).get(with(any(Class.class)));
        will(returnValue(messageHandler));

      }
    });

    singleNodeTableInterface = new ExplicitNodeCaller("fake", 0, c5ConnectionManager);

  }

  @After
  public void after() throws InterruptedException, IOException {

    context.checking(new Expectations() {
      {
        oneOf(c5ConnectionManager).close();
      }
    });

    singleNodeTableInterface.close();
  }

  @Test
  public void mutateMe() {

    Response response = new Response(Response.Command.MUTATE, 1l, null, new MutateResponse(null, true), null, null);
    context.checking(new Expectations() {
      {
        oneOf(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
        will(returnFutureWithValue(response));
      }
    });

    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        ByteBuffer.wrap(new byte[]{0x00}));

    MutateRequest mutateRequest = new MutateRequest(regionSpecifier, new MutationProto(), null);
    singleNodeTableInterface.mutate(mutateRequest);


    Condition condition = new Condition();
    mutateRequest = new MutateRequest(regionSpecifier, new MutationProto(), condition);
    context.checking(new Expectations() {
      {
        oneOf(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
        will(returnFutureWithValue(response));
      }
    });

    singleNodeTableInterface.mutate(mutateRequest);


  }

  @Test
  public void getMe()
      throws InterruptedException, ExecutionException, TimeoutException, IOException {

    Response response = new Response(Response.Command.GET, 1l, new GetResponse(null), null, null, null);
    context.checking(new Expectations() {
      {
        oneOf(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
        will(returnFutureWithValue(response));
      }
    });

    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        ByteBuffer.wrap(new byte[]{0x00}));

    Get get = new Get();
    GetRequest getRequest = new GetRequest(regionSpecifier, get);
    singleNodeTableInterface.get(getRequest);


  }


  @Test
  public void scanMe() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    SettableFuture<C5ClientScanner> scanFuture = SettableFuture.create();
    context.checking(new Expectations() {
      {
        oneOf(messageHandler).callScan(with(any(Call.class)), with(any((Channel.class))));
        will(returnValue(scanFuture));
      }
    });

    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        ByteBuffer.wrap(new byte[]{0x00}));

    Scan scan = new Scan();
    long scannerId = 100;
    int numberOfRows = 100;
    boolean closeScanner = false;
    long nextCallSeq = 101;

    ScanRequest scanRequest = new ScanRequest(regionSpecifier, scan, scannerId, numberOfRows, closeScanner, nextCallSeq);
    ListenableFuture<C5ClientScanner> f = singleNodeTableInterface.scan(scanRequest);

    List<Integer> cellsPerResult = new ArrayList<>();

    boolean moreResults = false;
    int ttl = 0;
    List<Result> results = new ArrayList<>();

    ByteBuffer cf = ByteBuffer.wrap(Bytes.toBytes("cf"));
    ByteBuffer cq = ByteBuffer.wrap(Bytes.toBytes("cq"));
    ByteBuffer value = ByteBuffer.wrap(Bytes.toBytes("value"));
    for (int i = 0; i != 10000; i++) {
      ByteBuffer row = ByteBuffer.wrap(Bytes.toBytes(i));
      results.add(new Result(Arrays.asList(new Cell(row, cf, cq, 0l, CellType.PUT, value)), 1, true));
    }

    ScanResponse scanResponse = new ScanResponse(cellsPerResult, scannerId, moreResults, ttl, results);

    Fiber fiber = new ThreadFiber();
    ChannelFuture channelFuture = context.mock(ChannelFuture.class);
    context.checking(new Expectations() {
      {
        allowing(channel).writeAndFlush(with(any(Object.class)));
        will(returnValue((channelFuture)));
      }
    });
    C5ClientScanner clientScanner = new C5QueueBasedClientScanner(channel, fiber, scannerId, 0l);

    scanFuture.set(clientScanner);
    C5ClientScanner clientClientScanner = f.get();
    clientClientScanner.add(scanResponse);

    results.stream().forEach(result -> {
      try {
        assertThat(clientClientScanner.next(), is(equalTo(result)));
      } catch (InterruptedException e) {
        assertTrue(1 == 0); // We should never throw an exception
      }
    });
    assertThat(clientClientScanner.next(), nullValue());
    clientClientScanner.close();

  }

  @Test
  public void multiMe() {
    List<RegionAction> regionActions = new ArrayList<>();
    MultiRequest multiRequest = new MultiRequest(regionActions);

    List<RegionActionResult> results = new ArrayList<>();
    MultiResponse multiResponse = new MultiResponse(results);

    Response response = new Response(Response.Command.MULTI, 1l, null, null, null, multiResponse);
    context.checking(new Expectations() {
      {
        oneOf(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
        will(returnFutureWithValue(response));
      }
    });
    singleNodeTableInterface.multiRequest(multiRequest);

  }

}
