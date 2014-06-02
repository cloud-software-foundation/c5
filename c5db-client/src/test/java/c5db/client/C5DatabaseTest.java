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
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
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
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

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
  private SettableFuture<Response> callFuture;


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
    callFuture = SettableFuture.create();
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
    context.checking(new Expectations() {
      {
        oneOf(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
        will(returnValue(callFuture));
      }
    });

    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        ByteBuffer.wrap(new byte[]{0x00}));

    MutateRequest mutateRequest = new MutateRequest(regionSpecifier, new MutationProto(), null);
    singleNodeTableInterface.mutate(mutateRequest);
    Response response = new Response(Response.Command.MUTATE, 1l, null, new MutateResponse(null, true), null, null);
    callFuture.set(response);

    Condition condition = new Condition();
    mutateRequest = new MutateRequest(regionSpecifier, new MutationProto(), condition);
    context.checking(new Expectations() {
      {
        oneOf(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
        will(returnValue(callFuture));
      }
    });

    singleNodeTableInterface.mutate(mutateRequest);
    response = new Response(Response.Command.MUTATE, 1l, null, new MutateResponse(null, true), null, null);
    callFuture.set(response);
  }

  @Test
  public void getMe()
      throws InterruptedException, ExecutionException, TimeoutException, IOException {
    context.checking(new Expectations() {
      {
        oneOf(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
        will(returnValue(callFuture));
      }
    });

    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        ByteBuffer.wrap(new byte[]{0x00}));

    Get get = new Get();
    GetRequest getRequest = new GetRequest(regionSpecifier, get);
    singleNodeTableInterface.get(getRequest);
    Response response = new Response(Response.Command.GET, 1l, new GetResponse(null), null, null, null);
    callFuture.set(response);
  }


  @Test
  public void scanMe() {
    context.checking(new Expectations() {
      {
        oneOf(messageHandler).callScan(with(any(Call.class)), with(any((Channel.class))));
        will(returnValue(callFuture));
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
    singleNodeTableInterface.scan(scanRequest);

    List<Integer> cellsPerResult = new ArrayList<>();

    boolean moreResults = false;
    int ttl = 0;
    List<Result> results = new ArrayList<>();
    ScanResponse scanResponse = new ScanResponse(cellsPerResult, scannerId, moreResults, ttl, results);

    Response response = new Response(Response.Command.SCAN, 1l, null, null, scanResponse, null);
    callFuture.set(response);
  }

  @Test
  public void multiMe() {
    context.checking(new Expectations() {
      {
        oneOf(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
        will(returnValue(callFuture));
      }
    });
    List<RegionAction> regionActions = new ArrayList<>();
    MultiRequest multiRequest = new MultiRequest(regionActions);
    singleNodeTableInterface.multiRequest(multiRequest);

    List<RegionActionResult> results = new ArrayList<>();
    MultiResponse multiResponse = new MultiResponse(results);
    Response response = new Response(Response.Command.MULTI, 1l, null, null, null, multiResponse);
    callFuture.set(response);
  }

}
