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
import c5db.client.generated.LocationResponse;
import c5db.client.generated.RegionLocation;
import c5db.client.generated.ScanRequest;
import c5db.interfaces.tablet.Tablet;
import c5db.regionserver.scan.ScanRunnable;
import io.netty.channel.ChannelHandlerContext;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.fibers.Fiber;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class RegionScanServerHandler {
  private final RegionServerService regionServerService;

  RegionScanServerHandler(RegionServerService regionServerService) {
    this.regionServerService = regionServerService;
  }

  void scan(ChannelHandlerContext ctx, Call call) throws Exception {
    final ScanRequest scanIn = call.getScan();
    if (scanIn == null) {
      throw new IOException("Poorly specified c5db.regionserver.scan. There is no actual get data in the RPC");
    }

    final long scannerId = getScannerId(scanIn);
    final Integer numberOfRowsToSend = scanIn.getNumberOfRows();
    Channel<Integer> channel = regionServerService.getScanManager().getChannel(scannerId);
    // New Scanner
    if (channel == null) {
      ByteBuffer startRow = call.getScan().getScan().getStartRow();
      // TODO make an issue that if it doesn't have a row, see if you have the next one locally
      Tablet tablet = this.regionServerService.getLocallyLeaderedTablet(call.getTableName(), startRow);

      HRegionInfo regionInfo = tablet.getRegionInfo();
      LocationResponse locationResponse = null;
      // TODO make an issue so that this should be done lazily and/or the last minute
      if (regionInfo.getEndKey() != null && regionInfo.getEndKey().length != 0) {
        RegionLocation nextTabletLocation = this.regionServerService.getNextLocalTabletLocation(tablet);
        if (nextTabletLocation != null) {
          locationResponse = new LocationResponse(Arrays.asList(nextTabletLocation), call);
        }
      }

      final ScanRunnable scanRunnable = new ScanRunnable(ctx, call, scannerId, tablet.getRegion(), locationResponse);
      final Fiber fiber = this.regionServerService.getNewFiber();
      fiber.start();
      channel = new MemoryChannel<>();
      channel.subscribe(fiber, scanRunnable);
      regionServerService.getScanManager().addChannel(scannerId, channel);
    }
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

}
