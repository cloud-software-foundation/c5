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
package c5db.regionserver;

import c5db.client.FakeHTable;
import c5db.client.ProtobufUtil;
import c5db.client.generated.Action;
import c5db.client.generated.ByteArrayComparable;
import c5db.client.generated.Call;
import c5db.client.generated.CompareType;
import c5db.client.generated.Condition;
import c5db.client.generated.Get;
import c5db.client.generated.GetRequest;
import c5db.client.generated.MultiRequest;
import c5db.client.generated.MutateRequest;
import c5db.client.generated.MutationProto;
import c5db.client.generated.RegionAction;
import c5db.client.generated.RegionActionResult;
import c5db.client.generated.RegionSpecifier;
import c5db.client.generated.Response;
import c5db.client.generated.Scan;
import c5db.client.generated.ScanRequest;
import c5db.interfaces.C5Server;
import c5db.interfaces.TabletModule;
import c5db.interfaces.tablet.Tablet;
import c5db.messages.generated.ModuleType;
import c5db.tablet.Region;
import c5db.util.ExceptionHandlingBatchExecutor;
import c5db.util.FiberSupplier;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetlang.fibers.PoolFiberFactory;
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
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

public class RegionServerTest {
  @Rule
  public JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(new Synchroniser());
  }};

  Tablet tablet = context.mock(Tablet.class);
  Region region = context.mock(Region.class);

  private final NioEventLoopGroup acceptConnectionGroup = new NioEventLoopGroup(1);
  private final NioEventLoopGroup ioWorkerGroup = new NioEventLoopGroup();
  private final ChannelHandlerContext ctx = context.mock(ChannelHandlerContext.class);
  private final TabletModule tabletModule = context.mock(TabletModule.class);
  private final PoolFiberFactory fiberFactory = new PoolFiberFactory(Executors.newFixedThreadPool(2));
  private final C5Server server = context.mock(C5Server.class);
  private final Random random = new Random();
  private final int port = 10000 + random.nextInt(100);

  private final FiberSupplier fiberSupplier = (throwableConsumer) ->
      fiberFactory.create(new ExceptionHandlingBatchExecutor(throwableConsumer));

  private RegionServerHandler regionServerHandler;
  RegionServerService regionServerService;

  @Before
  public void before() throws ExecutionException, InterruptedException {
    context.checking(new Expectations() {{
      oneOf(server).getFiberSupplier();
      will(returnValue(fiberSupplier));
    }});
    regionServerService = new RegionServerService(acceptConnectionGroup,
        ioWorkerGroup,
        port,
        server);

    SettableFuture<TabletModule> tabletModuleSettableFuture = SettableFuture.create();
    tabletModuleSettableFuture.set(tabletModule);
    context.checking(new Expectations() {{
      oneOf(server).getModule(with(any(ModuleType.class)));
      will(returnValue(tabletModuleSettableFuture));
    }});

    ListenableFuture<Service.State> future = regionServerService.start();
    future.get();
    regionServerHandler = new RegionServerHandler(regionServerService);
  }

  @After
  public void after() throws ExecutionException, InterruptedException {
    regionServerService.stop();
  }

  @Test(expected = RegionNotFoundException.class)
  public void shouldThrowErrorWhenInvalidRegionSpecifierSpecified() throws Exception {
    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME, null);
    Get get = new Get();
    GetRequest getRequest = new GetRequest(regionSpecifier, get);
    regionServerHandler.channelRead0(ctx, new Call(Call.Command.GET, 1, getRequest, null, null, null));
  }

  @Test(expected = IOException.class)
  public void shouldHandleGetCommandRequestWithNullArgument() throws Exception {
    regionServerHandler.channelRead0(ctx, new Call(Call.Command.GET, 1, null, null, null, null));
  }


  @Test(expected = IOException.class)
  public void shouldHandleMutateWithNullArguments() throws Exception {
    regionServerHandler.channelRead0(ctx, new Call(Call.Command.MUTATE, 1, null, null, null, null));
  }


  @Test(expected = IOException.class)
  public void shouldHandleMultiWithNullArgument() throws Exception {
    regionServerHandler.channelRead0(ctx, new Call(Call.Command.MULTI, 1, null, null, null, null));
  }


  @Test(expected = IOException.class)
  public void shouldHandleScanCommandRequestWithNullArgument() throws Exception {
    regionServerHandler.channelRead0(ctx, new Call(Call.Command.SCAN, 1, null, null, null, null));
  }

  @Test
  public void shouldBeAbleToScan() throws Exception {
    ByteBuffer regionLocation = ByteBuffer.wrap(Bytes.toBytes("testTable"));
    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        regionLocation);
    ScanRequest scanRequest = new ScanRequest(regionSpecifier, new Scan(), 10l, 10, false, 11l);
    RegionScanner regionScanner = context.mock(RegionScanner.class);
    context.checking(new Expectations() {{
      oneOf(tabletModule).getTablet("testTable");
      will(returnValue(tablet));

      oneOf(tablet).getRegion();
      will(returnValue(region));

      oneOf(region).getScanner(with(any(Scan.class)));
      will(returnValue(regionScanner));

      allowing(regionScanner).nextRaw(with(any(List.class)));
      will(returnValue(false));

      allowing(ctx).writeAndFlush(with(any(Response.class)));

      allowing(regionScanner).close();
    }});

    regionServerHandler.channelRead0(ctx, new Call(Call.Command.SCAN, 1, null, null, scanRequest, null));
  }

  @Test
  public void shouldBeAbleToHandleGet() throws Exception {
    ByteBuffer regionLocation = ByteBuffer.wrap(Bytes.toBytes("testTable"));
    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        regionLocation);


    Get get = ProtobufUtil.toGet(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("fakeRow")), false);
    GetRequest getRequest = new GetRequest(regionSpecifier, get);

    c5db.client.generated.Result result = new c5db.client.generated.Result();
    context.checking(new Expectations() {{
      oneOf(tabletModule).getTablet("testTable");
      will(returnValue(tablet));

      oneOf(tablet).getRegion();
      will(returnValue(region));

      oneOf(region).get(with(any(Get.class)));
      will(returnValue(result));

      oneOf(ctx).writeAndFlush(with(any(Response.class)));

    }});

    regionServerHandler.channelRead0(ctx, new Call(Call.Command.GET, 1, getRequest, null, null, null));
  }

  @Test
  public void shouldBeAbleToHandleExistsTrue() throws Exception {
    ByteBuffer regionLocation = ByteBuffer.wrap(Bytes.toBytes("testTable"));
    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        regionLocation);

    Get get = ProtobufUtil.toGet(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("fakeRow")), true);
    GetRequest getRequest = new GetRequest(regionSpecifier, get);

    context.checking(new Expectations() {{
      oneOf(tabletModule).getTablet("testTable");
      will(returnValue(tablet));

      oneOf(tablet).getRegion();
      will(returnValue(region));

      oneOf(region).exists(with(any(Get.class)));
      will(returnValue(true));

      oneOf(ctx).writeAndFlush(with(any(Response.class)));

    }});

    regionServerHandler.channelRead0(ctx, new Call(Call.Command.GET, 1, getRequest, null, null, null));
  }


  @Test
  public void shouldBeAbleToHandleExistsFalse() throws Exception {
    ByteBuffer regionLocation = ByteBuffer.wrap(Bytes.toBytes("testTable"));
    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        regionLocation);


    Get get = ProtobufUtil.toGet(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("fakeRow")), true);
    GetRequest getRequest = new GetRequest(regionSpecifier, get);

    context.checking(new Expectations() {{
      oneOf(tabletModule).getTablet("testTable");
      will(returnValue(tablet));

      oneOf(tablet).getRegion();
      will(returnValue(region));

      oneOf(region).exists(with(any(Get.class)));
      will(returnValue(false));

      oneOf(ctx).writeAndFlush(with(any(Response.class)));

    }});

    regionServerHandler.channelRead0(ctx, new Call(Call.Command.GET, 1, getRequest, null, null, null));
  }


  @Test
  public void shouldBeAbleToHandleMutate() throws Exception {
    ByteBuffer regionLocation = ByteBuffer.wrap(Bytes.toBytes("testTable"));
    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        regionLocation);

    MutationProto mutation = ProtobufUtil.toMutation(MutationProto.MutationType.PUT, new Put(Bytes.toBytes("fakeRow")));
    MutateRequest mutateRequest = new MutateRequest(regionSpecifier, mutation, new Condition());

    SettableFuture<Boolean> mutateSuccess = SettableFuture.create();

    context.checking(new Expectations() {{
      oneOf(tabletModule).getTablet("testTable");
      will(returnValue(tablet));

      oneOf(tablet).getRegion();
      will(returnValue(region));

      oneOf(region).batchMutate(with(any(MutationProto.class)));
      will(returnValue(mutateSuccess));
    }});
    regionServerHandler.channelRead0(ctx, new Call(Call.Command.MUTATE, 1, null, mutateRequest, null, null));

    context.checking(new Expectations() {
      {
        allowing(ctx).writeAndFlush(with(any(Response.class)));
      }
    });
    mutateSuccess.set(true);

  }

  @Test
  public void shouldBeAbleToHandleMutateWithCondition() throws Exception {
    ByteBuffer regionLocation = ByteBuffer.wrap(Bytes.toBytes("testTable"));
    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        regionLocation);

    MutationProto mutation = ProtobufUtil.toMutation(MutationProto.MutationType.PUT, new Put(Bytes.toBytes("fakeRow")));
    Condition condition = new Condition(ByteBuffer.wrap(Bytes.toBytes("row")),
        ByteBuffer.wrap(Bytes.toBytes("cf")),
        ByteBuffer.wrap(Bytes.toBytes("cq")),
        CompareType.EQUAL,
        FakeHTable.toComparator(new ByteArrayComparable(ByteBuffer.wrap(Bytes.toBytes("value")))));
    MutateRequest mutateRequest = new MutateRequest(regionSpecifier, mutation, condition);

    context.checking(new Expectations() {{
      oneOf(tabletModule).getTablet("testTable");
      will(returnValue(tablet));

      oneOf(tablet).getRegion();
      will(returnValue(region));

      oneOf(region).mutate(with(any(MutationProto.class)), with(any(Condition.class)));
      will(returnValue(true));

      oneOf(ctx).writeAndFlush(with(any(Response.class)));

    }});

    regionServerHandler.channelRead0(ctx, new Call(Call.Command.MUTATE, 1, null, mutateRequest, null, null));
  }

  @Test
  public void shouldBeAbleToHandleMulti() throws Exception {
    ByteBuffer regionLocation = ByteBuffer.wrap(Bytes.toBytes("testTable"));
    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        regionLocation);

    MutationProto mutation = ProtobufUtil.toMutation(MutationProto.MutationType.PUT, new Put(Bytes.toBytes("fakeRow")));
    Get get = ProtobufUtil.toGet(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("fakeRow")), false);

    List<RegionAction> regionActionList = new ArrayList<>();
    regionActionList.add(new RegionAction(regionSpecifier, true, Arrays.asList(new Action(0, mutation, null))));
    regionActionList.add(new RegionAction(regionSpecifier, false, Arrays.asList(new Action(1, mutation, null))));
    regionActionList.add(new RegionAction(regionSpecifier, true, Arrays.asList(new Action(2, null, get))));
    regionActionList.add(new RegionAction(regionSpecifier, false, Arrays.asList(new Action(3, mutation, get))));


    MultiRequest multiRequest = new MultiRequest(regionActionList);

    context.checking(new Expectations() {{
      exactly(4).of(tabletModule).getTablet("testTable");
      will(returnValue(tablet));

      exactly(4).of(tablet).getRegion();
      will(returnValue(region));

      exactly(4).of(region).processRegionAction(with(any(RegionAction.class)));
      will(returnValue(new RegionActionResult()));

      oneOf(ctx).writeAndFlush(with(any(Response.class)));

    }});

    regionServerHandler.channelRead0(ctx, new Call(Call.Command.MULTI, 1, null, null, null, multiRequest));
  }

}