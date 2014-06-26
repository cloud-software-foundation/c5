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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

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
        RegionGetServerHandler regionGetServerHandler = new RegionGetServerHandler(regionServerService);
        regionGetServerHandler.get(ctx, call);
        break;
      case MUTATE:
        RegionMutateServerHandler regionMutateServerHandler = new RegionMutateServerHandler(regionServerService);
        regionMutateServerHandler.mutate(ctx, call);
        break;
      case SCAN:
        RegionScanServerHandler regionScanServerHandler = new RegionScanServerHandler(regionServerService);
        regionScanServerHandler.scan(ctx, call);
        break;
      case MULTI:
        RegionMultiServerHandler regionMultiServerHandler = new RegionMultiServerHandler(regionServerService);
        regionMultiServerHandler.multi(ctx, call);
        break;
    }
  }


  static boolean regionSpecifierSupplied(Call call) {
    return false;

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