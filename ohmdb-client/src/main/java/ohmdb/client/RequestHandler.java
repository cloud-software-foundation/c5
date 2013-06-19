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
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;
import ohmdb.client.generated.ClientProtos;
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
  private final ConcurrentHashMap<Long, WickedQueue<ClientProtos.Result>>
      scanResults = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Long, Boolean> scansIsClosed
      = new ConcurrentHashMap<>();

  private final ConcurrentHashMap<Long, SettableFuture>
      futures = new ConcurrentHashMap<>();

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
        if (!msg.getScan().getMoreResults()) {
          scansIsClosed.put(msg.getScan().getScannerId(), true);
        }

        if (!scanResults.containsKey(msg.getScan().getScannerId())) {
          scanResults.put(msg.getScan().getScannerId(),
              new WickedQueue<ClientProtos.Result>(1000000));
          f.set(msg.getScan().getScannerId());
        }

        for (ClientProtos.Result result : msg.getScan().getResultList()) {
          scanResults.get(msg.getScan().getScannerId()).add(result);
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


  public ClientProtos.Result next(final long scannerId) throws IOException {
    ClientProtos.Result result;
    WickedQueue<ClientProtos.Result> queue = scanResults.get(scannerId);
    do {
      if (isClosed(scannerId)) {
        return null;
      }
      result = queue.poll();
    } while (result == null);
    return result;
  }

  public boolean isClosed(final long scannerId) {
    return scansIsClosed.containsKey(scannerId)
        && scansIsClosed.get(scannerId)
        && scanResults.get(scannerId).isEmpty();
  }
}
