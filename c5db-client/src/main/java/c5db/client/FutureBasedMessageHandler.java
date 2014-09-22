/*
 * Copyright 2014 WANdisco
 *
 *  WANdisco licenses this file to you under the Apache License,
 *  version 2.0 (the "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations
 *  under the License.
 */
package c5db.client;

import c5db.client.generated.Call;
import c5db.client.generated.Response;
import c5db.client.scanner.ClientScanner;
import c5db.client.scanner.ClientScannerManager;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A simple handler to handle inbound responses from the C5 server.
 */
public class FutureBasedMessageHandler extends SimpleChannelInboundHandler<Response> implements MessageHandler {
  private static final ClientScannerManager CLIENT_SCANNER_MANAGER = ClientScannerManager.INSTANCE;
  private final ConcurrentHashMap<Long, SettableFuture<Response>> futures = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Long, SettableFuture<Long>> scannerFutures = new ConcurrentHashMap<>();
  private final AtomicLong inFlightCalls = new AtomicLong(0);

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
        ClientScanner clientScanner;

        if (CLIENT_SCANNER_MANAGER.hasScanner(scannerId)) {
          clientScanner = CLIENT_SCANNER_MANAGER.get(scannerId).get();
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

    if (inFlightCalls.incrementAndGet() > C5Constants.IN_FLIGHT_CALLS) {
      System.out.println("Backing off:" + C5Constants.IN_FLIGHT_CALLS);
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
        System.exit(1);
      }
    }

    return settableFuture;
  }

  @Override
  public ListenableFuture<Long> callScan(final Call request, final Channel channel) {
    SettableFuture<Long> settableFuture = SettableFuture.create();
    scannerFutures.put(request.getCommandId(), settableFuture);
    channel.writeAndFlush(request);
    return settableFuture;
  }
}
