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
import c5db.client.generated.MutateRequest;
import c5db.client.generated.MutateResponse;
import c5db.client.generated.MutationProto;
import c5db.client.generated.Response;
import c5db.interfaces.tablet.Tablet;
import c5db.tablet.Region;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.netty.channel.ChannelHandlerContext;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class RegionMutateServerHandler {

  private final RegionServerService regionServerService;

  RegionMutateServerHandler(RegionServerService regionServerService) {
    this.regionServerService = regionServerService;
  }

  void mutate(ChannelHandlerContext ctx, Call call) throws RegionNotFoundException, IOException, InterruptedException, ExecutionException, TimeoutException, URISyntaxException, DeserializationException {
    final MutateRequest mutateIn = call.getMutate();
    if (mutateIn == null) {
      throw new IOException("Poorly specified mutate. There is no actual get data in the RPC");
    }
    ByteBuffer row = mutateIn.getMutation().getRow();
    Region region;
    if (RegionServerHandler.regionSpecifierSupplied(call)) {
      region = regionServerService.getRegion(call.getScan().getRegion());
    } else {
      long leader = regionServerService.leaderResultFromMeta(call.getTableName(), row);
      if (regionServerService.getServer().getNodeId() != leader) {
        throw new IOException("Unsupported");
      } else {
        Tablet tablet = regionServerService.getLocallyLeaderedTablet(call.getTableName(), row);
        region = tablet.getRegion();
      }
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
            null,
            null);
        ctx.writeAndFlush(response);
      }
    }
  }
}