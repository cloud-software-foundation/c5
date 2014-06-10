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

import c5db.client.generated.Call;
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
import c5db.client.generated.Response;
import c5db.client.generated.ScanRequest;
import c5db.regionserver.scan.ScanRunnable;
import c5db.tablet.Region;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.jetbrains.annotations.NotNull;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.fibers.Fiber;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The main netty handler for the RegionServer functionality. Maps protocol buffer calls to an action against a HRegion
 * and then provides a response to the caller.
 */
public class RegionServerHandler extends SimpleChannelInboundHandler<Call> {
  private final RegionServerService regionServerService;


  public RegionServerHandler(RegionServerService myService) {
    this.regionServerService = myService;
  }

  @Override
  public void channelRead0(final ChannelHandlerContext ctx, final Call call) throws Exception {
    switch (call.getCommand()) {
      case GET:
        get(ctx, call);
        break;
      case MUTATE:
        mutate(ctx, call);
        break;
      case SCAN:
        scan(ctx, call);
        break;
      case MULTI:
        multi(ctx, call);
        break;
    }
  }

  private void multi(ChannelHandlerContext ctx, Call call) throws IOException, RegionNotFoundException {
    final MultiRequest request = call.getMulti();

    List<RegionActionResult> regionActionResults = new ArrayList<>();
    if (request == null) {
      throw new IOException("Poorly specified multi. There is no actual get data in the RPC");
    }
    for (RegionAction regionAction : request.getRegionActionList()) {
      final Region region = regionServerService.getOnlineRegion(regionAction.getRegion());
      RegionActionResult regionActionResponse = region.processRegionAction(regionAction);
      regionActionResults.add(regionActionResponse);
    }
    MultiResponse multiResponse = new MultiResponse(regionActionResults);
    final Response response = new Response(Response.Command.MULTI,
        call.getCommandId(),
        null,
        null,
        null,
        multiResponse);
    ctx.writeAndFlush(response);
  }

  private void mutate(ChannelHandlerContext ctx, Call call) throws RegionNotFoundException, IOException {
    final MutateRequest mutateIn = call.getMutate();

    if (mutateIn == null) {
      throw new IOException("Poorly specified mutate. There is no actual get data in the RPC");
    }

    final Region region = regionServerService.getOnlineRegion(call.getMutate().getRegion());
    if (mutateIn.getMutation().getMutateType().equals(MutationProto.MutationType.PUT) &&
        (mutateIn.getCondition() == null || mutateIn.getCondition().getRow() == null)) {
      Futures.addCallback(region.batchMutate(mutateIn.getMutation()), new FutureCallback<Boolean>() {
        @Override
        public void onSuccess(@NotNull Boolean result) {
          MutateResponse mutateResponse = new MutateResponse(new c5db.client.generated.Result(), true);
          final Response response = new Response(Response.Command.MUTATE,
              call.getCommandId(),
              null,
              mutateResponse,
              null,
              null);
          ctx.writeAndFlush(response);

        }

        @Override
        public void onFailure(@NotNull Throwable t) {
        }
      });
      //TODO check success

    } else {
      boolean success = region.mutate(mutateIn.getMutation(), mutateIn.getCondition());

      //TODO check success
      MutateResponse mutateResponse = new MutateResponse(new c5db.client.generated.Result(), success);

      final Response response = new Response(Response.Command.MUTATE,
          call.getCommandId(),
          null,
          mutateResponse,
          null,
          null);
      ctx.writeAndFlush(response);
    }
  }


  private void scan(ChannelHandlerContext ctx, Call call) throws Exception {
    final ScanRequest scanIn = call.getScan();
    if (scanIn == null) {
      throw new IOException("Poorly specified c5db.regionserver.scan. There is no actual get data in the RPC");
    }

    final long scannerId = getScannerId(scanIn);
    final Integer numberOfRowsToSend = scanIn.getNumberOfRows();
    Channel<Integer> channel = regionServerService.getScanManager().getChannel(scannerId);
    // New Scanner
    if (null == channel) {
      final Fiber fiber = this.regionServerService.getNewFiber();
      fiber.start();
      channel = new MemoryChannel<>();
      Region region = regionServerService.getOnlineRegion(call.getScan().getRegion());
      final ScanRunnable scanRunnable = new ScanRunnable(ctx, call, scannerId, region);
      channel.subscribe(fiber, scanRunnable);
      regionServerService.getScanManager().addChannel(scannerId, channel);
    }
    // TODO receive exceptions
    channel.publish(numberOfRowsToSend);
  }

  private long getScannerId(ScanRequest scanIn) {
    if (scanIn.getScannerId() > 0) {
      return scanIn.getScannerId();
    } else {
      // Make a scanner with an Id not 0
      return this.regionServerService.scannerCounter.incrementAndGet();
    }
  }

  private void get(ChannelHandlerContext ctx, Call call) throws IOException, RegionNotFoundException {
    final GetRequest getRequest = call.getGet();
    if (getRequest == null) {
      throw new IOException("Poorly specified getRequest. There is no actual get data in the RPC");
    }
    final Get getIn = getRequest.getGet();

    final Region region = regionServerService.getOnlineRegion(call.getGet().getRegion());
    if (region == null) {
      throw new IOException("Unable to find region");
    }

    if (getIn.getExistenceOnly()) {
      final boolean exists = region.exists(getRequest.getGet());
      final GetResponse getResponse = new GetResponse(new c5db.client.generated.Result(new ArrayList<>(), 0, exists));
      final Response response = new Response(Response.Command.GET, call.getCommandId(), getResponse, null, null, null);
      ctx.writeAndFlush(response);
    } else {
      final c5db.client.generated.Result getResult = region.get(getRequest.getGet());
      final GetResponse getResponse = new GetResponse(getResult);
      final Response response = new Response(Response.Command.GET, call.getCommandId(), getResponse, null, null, null);
      ctx.writeAndFlush(response);
    }
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    ctx.flush();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
      throws Exception {
    super.exceptionCaught(ctx, cause);

  }
}