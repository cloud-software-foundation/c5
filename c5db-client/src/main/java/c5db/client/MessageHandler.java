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
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import c5db.client.scanner.ClientScanner;
import c5db.client.scanner.ClientScannerManager;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class MessageHandler extends SimpleChannelInboundHandler<ClientProtos.Response> {
  private final static ClientScannerManager clientScanManager = ClientScannerManager.INSTANCE;
  private final ConcurrentHashMap<Long, SettableFuture> futures = new ConcurrentHashMap<>();

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ClientProtos.Response msg) throws Exception {
    final SettableFuture f = futures.get(msg.getCommandId());
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

        if (clientScanManager.hasScanner(scannerId)) {
          clientScanner = clientScanManager.get(scannerId);
        } else {
          clientScanner = clientScanManager.createAndGet(ctx.channel(), scannerId);
          f.set(scannerId);
        }

        clientScanner.add(msg.getScan());

        if (!msg.getScan().getMoreResults()) {
          clientScanner.close();
        }

        break;
      default:
        f.set(msg);
        break;
    }
  }

  public void call(final ClientProtos.Call request, final SettableFuture future, final Channel channel)
      throws InterruptedException, IOException {
    futures.put(request.getCommandId(), future);
    channel.writeAndFlush(request);
  }
}