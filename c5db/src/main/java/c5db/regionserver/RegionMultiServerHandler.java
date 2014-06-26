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

import c5db.client.generated.Action;
import c5db.client.generated.Call;
import c5db.client.generated.Get;
import c5db.client.generated.MultiRequest;
import c5db.client.generated.MultiResponse;
import c5db.client.generated.MutationProto;
import c5db.client.generated.NameBytesPair;
import c5db.client.generated.RegionActionResult;
import c5db.client.generated.Response;
import c5db.client.generated.ResultOrException;
import c5db.tablet.Region;
import io.netty.channel.ChannelHandlerContext;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RegionMultiServerHandler {
  private final RegionServerService regionServerService;

  RegionMultiServerHandler(RegionServerService regionServerService) {
    this.regionServerService = regionServerService;
  }

  // TODO multi needs a bunch of work deciding exactly how we want to decide what row to use
  protected void multi(ChannelHandlerContext ctx, Call call) throws IOException, RegionNotFoundException {
    final MultiRequest request = call.getMulti();

    List<RegionActionResult> regionActionResults = new ArrayList<>();

    if (request == null) {
      throw new IOException("Poorly specified multi. There is no actual get data in the RPC");
    }
    List<ResultOrException> throwables = new ArrayList<>();
    request
        .getRegionActionList()
        .parallelStream()
        .forEach(regionAction -> {
          Action action = regionAction.getActionList().iterator().next();
          if (RegionServerHandler.regionSpecifierSupplied(call)) {
          } else {
            Get get = action.getGet();

            ByteBuffer row = null;
            if (get != null) {
              row = get.getRow();
            }

            MutationProto mutation = action.getMutation();
            if (row != null && mutation != null && row != mutation.getRow()) {
              throwables.add(new ResultOrException(throwables.size(), null, new NameBytesPair("Multi row mutation", null)));
            } else if (mutation != null) {
              row = mutation.getRow();
            } else if (row == null) {
              throwables.add(new ResultOrException(throwables.size(), null, new NameBytesPair("We have a null row", null)));
            }

            Region region = null;
            try {
              region = this.regionServerService.getLocallyLeaderedTablet(call.getTableName(), row).getRegion();
            } catch (RegionNotFoundException e) {
              throwables.add(new ResultOrException(throwables.size(), null, new NameBytesPair(e.getLocalizedMessage(), null)));
            }

            if (region == null) {
              throwables.add(new ResultOrException(throwables.size(), null, new NameBytesPair("We returned a null region", null)));

            } else {
              String row1 = Bytes.toString(row.array());
              RegionActionResult regionActionResponse = region.processRegionAction(regionAction);
              regionActionResults.add(regionActionResponse);
            }
            regionActionResults.add(new RegionActionResult(throwables, null));
          }
        });
    MultiResponse multiResponse = new MultiResponse(regionActionResults);
    final Response response = new Response(Response.Command.MULTI,
        call.getCommandId(),
        null,
        null,
        null,
        multiResponse,
        null);
    ctx.writeAndFlush(response);
  }
}
