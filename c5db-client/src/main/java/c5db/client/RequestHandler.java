/*
 * Copyright (C) 2013  Ohm Data
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
 *
 *  This file incorporates work covered by the following copyright and
 *  permission notice:
 */
package c5db.client;

import c5db.client.generated.ClientProtos;
import c5db.client.scanner.ClientScanner;
import c5db.client.scanner.ClientScannerManager;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class RequestHandler
    extends SimpleChannelInboundHandler<ClientProtos.Response> {
  private final ConcurrentHashMap<Long, SettableFuture> futures = new ConcurrentHashMap<>();
  ClientScannerManager manager = ClientScannerManager.INSTANCE;
  public static final Log LOG = LogFactory.getLog(RequestHandler.class);

  @Override
  public void channelRead0(final ChannelHandlerContext ctx, final ClientProtos.Response msg)
      throws Exception {
    SettableFuture f = futures.get(msg.getCommandId());
    switch (msg.getCommand()) {
      case MUTATE:
        if (!msg.getMutate().getProcessed()) {
          IOException exception = new IOException("Not Processed");
          f.setException(exception);
        }
        f.set(null);
        break;
      case SCAN:
        long scannerId = msg.getScan().getScannerId();

        ClientScanner clientScanner;
        if (!manager.hasScanner(scannerId)) {
          clientScanner = manager.getOrCreate(scannerId);
          f.set(scannerId);
        } else {
          clientScanner = manager.getOrCreate(scannerId);
        }
        clientScanner.add(msg.getScan());
        if (!msg.getScan().getMoreResults()) {
          clientScanner.close();
        }
        break;
      default:
        LOG.error("msg:" +msg);
        f.set(msg);
        break;
    }
  }

  public void call(final ClientProtos.Call request, final SettableFuture future, final Channel channel)
      throws InterruptedException, IOException {
    futures.put(request.getCommandId(), future);
    channel.writeAndFlush(request);
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }
}