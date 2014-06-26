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
import c5db.client.generated.LocationResponse;
import c5db.client.generated.RegionLocation;
import c5db.client.generated.Response;
import c5db.tablet.Region;
import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class RegionGetServerHandler {
  private final RegionServerService regionServerService;

  RegionGetServerHandler(RegionServerService regionServerService) {
    this.regionServerService = regionServerService;
  }

  void get(ChannelHandlerContext ctx, Call call) throws IOException, RegionNotFoundException, InterruptedException, ExecutionException, TimeoutException {
      final GetRequest getRequest = call.getGet();
      if (getRequest == null) {
        throw new IOException("Poorly specified getRequest. There is no actual get data in the RPC");
      }
      final Get getIn = getRequest.getGet();
    try {
      Region region;
      if (RegionServerHandler.regionSpecifierSupplied(call)) {
        region = regionServerService.getRegion(call.getGet().getRegion());
      } else {
        region = regionServerService.getLocallyLeaderedTablet(call.getTableName(), getIn.getRow()).getRegion();
      }

      if (region == null) {
        throw new IOException("Unable to find region");
      }

      if (getIn.getExistenceOnly()) {
        final boolean exists = region.exists(getRequest.getGet());
        final GetResponse getResponse = new GetResponse(new c5db.client.generated.Result(new ArrayList<>(), 0, exists));
        final Response response = new Response(Response.Command.GET, call.getCommandId(), getResponse, null, null, null, null);
        ctx.writeAndFlush(response);
      } else {
        final c5db.client.generated.Result getResult = region.get(getRequest.getGet());
        final GetResponse getResponse = new GetResponse(getResult);
        final Response response = new Response(Response.Command.GET, call.getCommandId(), getResponse, null, null, null, null);
        ctx.writeAndFlush(response);
      }
      } catch (RegionNotFoundException e) {
      List<RegionLocation> locations = new ArrayList<>();
      LocationResponse locationResponse = new LocationResponse(locations, call);
      final Response response = new Response(Response.Command.LOCATION, call.getCommandId(), null, null, null, null, locationResponse);
      ctx.writeAndFlush(response);

    }
  }

}
