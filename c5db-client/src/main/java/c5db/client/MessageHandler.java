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
import c5db.client.generated.Response;
import c5db.client.scanner.ClientScanner;
import c5db.client.scanner.ClientScannerManager;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/***
 * A simple handler to handle inbound responses from the C5 server.
 */
public class MessageHandler extends SimpleChannelInboundHandler<Response> {
  private static final ClientScannerManager CLIENT_SCANNER_MANAGER = ClientScannerManager.INSTANCE;
  private final ConcurrentHashMap<Long, SettableFuture<Response>> futures = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Long, SettableFuture<Long>> scannerFutures = new ConcurrentHashMap<>();


  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Response msg) throws Exception {
    switch (msg.getCommand()) {
      case MUTATE:
        final SettableFuture<Response> f = futures.get(msg.getCommandId());
        f.set(msg) ;
        break;
      case SCAN:
        final long scannerId = msg.getScan().getScannerId();
        ClientScanner clientScanner;

        if (CLIENT_SCANNER_MANAGER.hasScanner(scannerId)) {
          clientScanner = CLIENT_SCANNER_MANAGER.get(scannerId);
        } else {
          clientScanner = CLIENT_SCANNER_MANAGER.createAndGet(ctx.channel(), scannerId, msg.getCommandId());
          scannerFutures.get(msg.getCommandId()).set(scannerId);
        }

        clientScanner.add(msg.getScan());

        if (!msg.getScan().getMoreResults()) {
          clientScanner.close();
        }
        break;
      default:
        futures.get(msg.getCommandId()).set(msg);
        break;
    }
  }

  public void call(final Call request, final SettableFuture<Response> future, final Channel channel) {
    futures.put(request.getCommandId(), future);
    channel.writeAndFlush(request);
  }

  public void callScan(final Call request, final SettableFuture<Long> future, final Channel channel) {
    scannerFutures.put(request.getCommandId(), future);
    channel.writeAndFlush(request);
  }
}
