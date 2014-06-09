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
import c5db.client.scanner.C5ClientScanner;
import c5db.client.scanner.C5QueueBasedClientScanner;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A simple handler to handle inbound responses from the C5 server.
 */
public class FutureBasedMessageHandler extends SimpleChannelInboundHandler<Response> implements MessageHandler {
  private final ConcurrentHashMap<Long, SettableFuture<Response>> futures = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Long, SettableFuture<Long>> scannerFutures = new ConcurrentHashMap<>();
  private final AtomicLong inFlightCalls = new AtomicLong(0);

  private final ExecutorService executorService = Executors.newSingleThreadExecutor();
  private final PoolFiberFactory poolFiberFactory = new PoolFiberFactory(executorService);

  private final ClientScannerManager clientScannerManager = new ClientScannerManager(poolFiberFactory.create());

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Response msg) throws Exception {
    switch (msg.getCommand()) {
      case MULTI:
        futures.get(msg.getCommandId()).set(msg);
        break;
      case MUTATE:
        futures.get(msg.getCommandId()).set(msg);
        break;
      case SCAN:
        final long scannerId = msg.getScan().getScannerId();
        C5ClientScanner clientScanner;

        if (clientScannerManager.hasScanner(scannerId)) {
          clientScanner = clientScannerManager.get(scannerId).get();
        } else {
          clientScanner = clientScannerManager.createAndGet(ctx.channel(), scannerId, msg.getCommandId());
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

  @Override
  public ListenableFuture<Response> call(final Call request, final Channel channel) {
    SettableFuture<Response> settableFuture = SettableFuture.create();
    futures.put(request.getCommandId(), settableFuture);
    channel.writeAndFlush(request);
    return settableFuture;
  }

  @Override
  public ListenableFuture<Response> buffer(final Call request, final Channel channel) {
    SettableFuture<Response> settableFuture = SettableFuture.create();
    futures.put(request.getCommandId(), settableFuture);
    // Keep track of how many outstanding requests we have and limit it.
    ChannelFuture future = channel.write(request);
    future.addListener(objectFuture -> inFlightCalls.decrementAndGet());
    if (inFlightCalls.get() > C5Constants.IN_FLIGHT_CALLS) {
      future.awaitUninterruptibly();
    }

    return settableFuture;
  }

  @Override
  public ListenableFuture<C5ClientScanner> callScan(final Call request, final Channel channel)
      throws InterruptedException, ExecutionException, TimeoutException {

    SettableFuture<Long> settableFuture = SettableFuture.create();
    scannerFutures.put(request.getCommandId(), settableFuture);
    channel.writeAndFlush(request);
    Long scannerId = settableFuture.get(C5Constants.CREATE_SCANNER_TIMEOUT, TimeUnit.MILLISECONDS);
    return clientScannerManager.get(scannerId);
  }

  class ClientScannerManager {

    private final Fiber fiber;

    ClientScannerManager(Fiber fiber) {
      this.fiber = fiber;
    }

    private final ConcurrentHashMap<Long, SettableFuture<C5ClientScanner>> scannerMap = new ConcurrentHashMap<>();

    public C5ClientScanner createAndGet(Channel channel, long scannerId, long commandId) throws IOException {
      if (hasScanner(scannerId)) {
        throw new IOException("Scanner already created");
      }

      final C5ClientScanner scanner = new C5QueueBasedClientScanner(channel, fiber, scannerId, commandId);
      SettableFuture<C5ClientScanner> clientScannerSettableFuture = SettableFuture.create();
      clientScannerSettableFuture.set(scanner);
      scannerMap.put(scannerId, clientScannerSettableFuture);
      return scanner;
    }

    public ListenableFuture<C5ClientScanner> get(long scannerId) {
      return scannerMap.get(scannerId);
    }

    public boolean hasScanner(long scannerId) {
      return scannerMap.containsKey(scannerId);
    }
  }
}
