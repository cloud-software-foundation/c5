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
 */
package c5db.client;

import c5db.client.generated.Action;
import c5db.client.generated.Call;
import c5db.client.generated.GetRequest;
import c5db.client.generated.LocationRequest;
import c5db.client.generated.MultiRequest;
import c5db.client.generated.MutateRequest;
import c5db.client.generated.Response;
import c5db.client.generated.ScanRequest;
import c5db.client.generated.TableName;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.channel.Channel;
import org.jetbrains.annotations.NotNull;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static c5db.client.generated.Call.Command.GET;
import static c5db.client.generated.Call.Command.MULTI;
import static c5db.client.generated.Call.Command.MUTATE;
import static c5db.client.generated.Call.Command.SCAN;
import static java.util.concurrent.TimeUnit.MILLISECONDS;


/**
 * The main client entry point for putting data into C5. Equivalent to HTablet from HBase.
 */
public class ExplicitNodeCaller implements TableInterface {
  private static final Logger LOG = LoggerFactory.getLogger(ExplicitNodeCaller.class);
  private final AtomicLong commandId = new AtomicLong(0);
  private final C5ConnectionManager c5ConnectionManager;
  private final Channel channel;
  private final MessageHandler handler;

  /**
   * C5Table is the main entry points for clients of C5DB
   */

  public ExplicitNodeCaller(String hostname, int port)
      throws InterruptedException, ExecutionException, TimeoutException, URISyntaxException {
    this(hostname, port, new C5NettyConnectionManager());
  }

  public ExplicitNodeCaller(String hostname, int port, C5ConnectionManager c5ConnectionManager)
      throws InterruptedException, ExecutionException, TimeoutException, URISyntaxException {
    // TODO Route data so we don't need to connect to meta
    this.c5ConnectionManager = c5ConnectionManager;
    this.channel = c5ConnectionManager.getOrCreateChannel(hostname, port);
    this.handler = channel.pipeline().get(FutureBasedMessageHandler.class);

    Fiber flusher = new ThreadFiber();
    flusher.start();
    flusher.scheduleAtFixedRate(this::flushHandler, 0, 500, MILLISECONDS);
  }

  @Override
  public ListenableFuture<Response>
  get(@NotNull final TableName tableName, @NotNull final GetRequest get)
      throws InterruptedException, ExecutionException {
    return handler.call(new Call(GET, commandId.incrementAndGet(), get, null, null, null, tableName),
        getChannelFor(tableName, get.getGet().getRow()));
  }

  @Override
  public ListenableFuture<c5db.client.scanner.C5ClientScanner>
  scan(@NotNull final TableName tableName, @NotNull ScanRequest scanRequest)
      throws InterruptedException, ExecutionException {
    ByteBuffer startRow = scanRequest.getScan().getStartRow();
    return handler.callScan(new Call(SCAN, commandId.incrementAndGet(), null, null, scanRequest, null, tableName),
        getChannelFor(tableName, startRow));
  }

  @Override
  public ListenableFuture<Response>
  mutate(@NotNull final TableName tableName, @NotNull final MutateRequest mutateRequest)
      throws InterruptedException, ExecutionException {
    return handler.buffer(new Call(MUTATE, commandId.incrementAndGet(), null, mutateRequest, null, null, tableName),
        getChannelFor(tableName, mutateRequest.getMutation().getRow()));
  }

  @Override
  public ListenableFuture<Response>
  multiRequest(@NotNull final TableName tableName, @NotNull final MultiRequest multiRequest)
      throws InterruptedException, ExecutionException {
    try {
      Action firstAction = multiRequest.getRegionActionList().get(0).getActionList().get(0);
      return handler.call(new Call(MULTI, commandId.incrementAndGet(), null, null, null, multiRequest, tableName),
          getChannelFor(tableName, getRowFromAction(firstAction)));
    } catch (IndexOutOfBoundsException e) {
      throw new ExecutionException(e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      c5ConnectionManager.close();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }


  public void flushHandler() {
    channel.flush();
  }

  private
  @NotNull
  Channel getChannelFor(TableName tableName, ByteBuffer row)
      throws InterruptedException, ExecutionException {
    LocationRequest locationRequest = new LocationRequest(tableName, row);
    return channel;
  }

  private ByteBuffer getRowFromAction(final Action firstAction) {
    if (firstAction.getMutation() != null &&
        firstAction.getMutation().getRow() != null &&
        firstAction.getMutation().getRow().array().length != 0) {
      return firstAction.getMutation().getRow();

    } else if (firstAction.getGet() != null &&
        firstAction.getGet().getRow() != null &&
        firstAction.getGet().getRow().array().length != 0) {
      return firstAction.getGet().getRow();
    } else {
      LOG.error("Unable to find a row to stage the commit, returning bogus row");
      return ByteBuffer.wrap(new byte[]{0x00});
    }
  }

}
