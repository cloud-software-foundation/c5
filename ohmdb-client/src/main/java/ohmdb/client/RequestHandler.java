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
package ohmdb.client;

import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;
import ohmdb.client.generated.ClientProtos;
import ohmdb.client.generated.HBaseProtos;
import ohmdb.client.queue.WickedQueue;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RequestHandler
    extends ChannelInboundMessageHandlerAdapter<ClientProtos.Response> {
  private Channel channel;

  private static final Logger logger = Logger.getLogger(
      RequestHandler.class.getName());

  private final ConcurrentHashMap<Long, SettableFuture>
      futures = new ConcurrentHashMap<>();

  ClientScannerManager manager = ClientScannerManager.INSTANCE;

  @Override
  public void messageReceived(final ChannelHandlerContext ctx,
                              final ClientProtos.Response msg)
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
        if (!manager.hasScanner(scannerId)){
          clientScanner =  manager.getOrCreate(scannerId);
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
        f.set(msg);
        break;
    }
  }

  public void call(ClientProtos.Call request,
                   SettableFuture future)
      throws InterruptedException, IOException {
    futures.put(request.getCommandId(), future);
    channel.write(request);
    channel.flush();
  }

  @Override
  public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
    channel = ctx.channel();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
      throws Exception {
    logger.log(
        Level.WARNING,
        "Unexpected exception from downstream.", cause);
    ctx.close();
  }
}