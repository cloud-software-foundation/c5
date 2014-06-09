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
import c5db.client.generated.Cell;
import c5db.client.generated.CellType;
import c5db.client.generated.MutateResponse;
import c5db.client.generated.Response;
import c5db.client.scanner.C5ClientScanner;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.protostuff.ByteString;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.util.Bytes;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static c5db.FutureActions.returnFutureWithValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertTrue;

public class C5FakeHTableTest {

  @Rule
  public JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(new Synchroniser());
  }};

  private final ExecutorService executorService = Executors.newSingleThreadExecutor();
  private final MessageHandler messageHandler = context.mock(MessageHandler.class);
  private final ChannelPipeline channelPipeline = context.mock(ChannelPipeline.class);
  private final C5ConnectionManager c5ConnectionManager = context.mock(C5ConnectionManager.class);
  private final Channel channel = context.mock(Channel.class);
  private final byte[] row = Bytes.toBytes("row");
  private final byte[] cf = Bytes.toBytes("cf");
  private final byte[] cq = Bytes.toBytes("cq");
  private final byte[] value = Bytes.toBytes("value");
  private ExplicitNodeCaller singleNodeTableInterface;
  private FakeHTable hTable;

  @Before
  public void before() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    context.checking(new Expectations() {
      {
        oneOf(c5ConnectionManager).getOrCreateChannel(with(any(String.class)), with(any(int.class)));
        will(returnValue(channel));

        oneOf(channel).pipeline();
        will(returnValue(channelPipeline));

        oneOf(channelPipeline).get(with(any(Class.class)));
        will(returnValue(messageHandler));

      }
    });

    singleNodeTableInterface = new ExplicitNodeCaller("fake", 0, c5ConnectionManager);
    hTable = new FakeHTable(singleNodeTableInterface, ByteString.copyFromUtf8("Does Not Exist"));
  }

  @After
  public void after() throws InterruptedException, IOException {

    context.checking(new Expectations() {
      {
        oneOf(c5ConnectionManager).close();
      }
    });

    singleNodeTableInterface.close();
  }

  @Test(expected = IOException.class)
  public void putShouldErrorOnInvalidResponse() throws IOException, InterruptedException, ExecutionException, TimeoutException, MutationFailedException {
    context.checking(new Expectations() {
      {
        oneOf(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
        will(returnFutureWithValue(new Response()));
      }
    });
    hTable.put(new Put(row));

  }

  @Test(expected = IOException.class)
  public void putShouldThrowErrorIfMutationFailed()
      throws InterruptedException, ExecutionException, TimeoutException, MutationFailedException, IOException {
    Response response = new Response(Response.Command.MUTATE, 1l, null, new MutateResponse(null, false), null, null);

    context.checking(new Expectations() {
      {
        oneOf(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
        will(returnFutureWithValue(response));
      }
    });
    hTable.put(new Put(row));
  }

  @Test
  public void putCanSucceed()
      throws InterruptedException, ExecutionException, TimeoutException, IOException {
    Response response = new Response(Response.Command.MUTATE, 1l, null, new MutateResponse(null, true), null, null);

    context.checking(new Expectations() {
      {
        oneOf(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
        will(returnFutureWithValue(response));
      }
    });
    hTable.put(new Put(row));
  }

  @Test(expected = TimeoutException.class)
  public void manyPutsBlocksIfNotEnoughRoom()
      throws InterruptedException, ExecutionException, TimeoutException, IOException {
    hTable.setAutoFlush(false);

    try {
      long messagesToPut = hTable.getWriteBufferSize() + 1;
      for (int i = 0; i != messagesToPut; i++) {
        SettableFuture<Response> response = SettableFuture.create();
        context.checking(new Expectations() {
          {
            allowing(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
            will(returnValue(response));
          }
        });
        executorService.submit(() -> {
          hTable.put(new Put(row));
          return null;
        }).get(2, TimeUnit.SECONDS);
      }
      // We should never get here
      assertTrue(1 == 0);
    } finally {
      hTable.setAutoFlush(true);
    }
  }

  @Test(expected = TimeoutException.class)
  public void testManyPutsFailsWithoutResponseWithAutoFlush()
      throws InterruptedException, ExecutionException, TimeoutException, IOException {
    ExecutorService executorService = Executors.newSingleThreadExecutor();

    long messagesToPut = 2;
    for (int i = 0; i != messagesToPut; i++) {
      SettableFuture<Response> response = SettableFuture.create();
      context.checking(new Expectations() {
        {
          allowing(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
          will(returnValue(response));
        }
      });
      executorService.submit(() -> {
        hTable.put(new Put(row));
        return null;
      }).get(2, TimeUnit.SECONDS);
    }
    // We should never get here
    assertTrue(1 == 0);
  }

  @Test(expected = TimeoutException.class)
  public void testFlushCommittedWillBlock()
      throws InterruptedException, ExecutionException, TimeoutException, IOException {

    hTable.setAutoFlush(false);
    try {
      long messagesToPut = 100;
      hTable.setWriteBufferSize(messagesToPut);
      ArrayBlockingQueue<SettableFuture<Response>> futures = new ArrayBlockingQueue<>((int) messagesToPut);

      for (int i = 0; i != messagesToPut; i++) {
        SettableFuture<Response> response = SettableFuture.create();
        context.checking(new Expectations() {
          {
            allowing(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
            will(returnValue(response));
          }
        });
        hTable.put(new Put(row));
        futures.add(response);
      }
      Future<Object> flushFuture = executorService.submit(() -> {
        hTable.flushCommits();
        return null;
      });

      // Remove one entry from the top
      futures.remove();
      Response response = new Response(Response.Command.MUTATE, 1l, null, new MutateResponse(null, true), null, null);
      futures.parallelStream().forEach(responseSettableFuture -> responseSettableFuture.set(response));

      flushFuture.get(2, TimeUnit.SECONDS);
    } finally {
      hTable.setAutoFlush(true);
    }
  }

  @Test
  public void testFlushWillClear()
      throws InterruptedException, ExecutionException, TimeoutException, IOException {

    hTable.setAutoFlush(false);
    try {
      long messagesToPut = 100;
      hTable.setWriteBufferSize(messagesToPut);
      ArrayBlockingQueue<SettableFuture<Response>> futures = new ArrayBlockingQueue<>((int) messagesToPut);
      for (int i = 0; i != messagesToPut; i++) {
        SettableFuture<Response> response = SettableFuture.create();
        context.checking(new Expectations() {
          {
            allowing(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
            will(returnValue(response));
          }
        });
        hTable.put(new Put(row));
        futures.add(response);
      }

      Future<Object> flushFuture = executorService.submit(() -> {
        hTable.flushCommits();
        return null;
      });

      Response response = new Response(Response.Command.MUTATE, 1l, null, new MutateResponse(null, true), null, null);
      futures.parallelStream().forEach(responseSettableFuture -> responseSettableFuture.set(response));
      flushFuture.get();

    } finally {
      hTable.setAutoFlush(true);
    }
  }


  @Test
  public void manyPutsDoNotBlockIfRoom()
      throws InterruptedException, ExecutionException, TimeoutException, IOException {

    hTable.setAutoFlush(false);

    try {
      int messagesToPut = 999;
      ArrayBlockingQueue<SettableFuture<Response>> futures = new ArrayBlockingQueue<>(messagesToPut);
      hTable.setWriteBufferSize(messagesToPut);
      for (int i = 0; i != messagesToPut; i++) {
        SettableFuture<Response> response = SettableFuture.create();

        context.checking(new Expectations() {
          {
            oneOf(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
            will(returnValue(response));
          }
        });
        hTable.put(new Put(row));
        futures.add(response);
      }
      Response response = new Response(Response.Command.MUTATE, 1l, null, new MutateResponse(null, true), null, null);
      futures.parallelStream().forEach(responseSettableFuture -> responseSettableFuture.set(response));
    } finally {
      hTable.setAutoFlush(true);
    }
  }


  @Test
  public void putsCanSucceed()
      throws InterruptedException, ExecutionException, TimeoutException, IOException {
    Response response = new Response(Response.Command.MUTATE, 1l, null, new MutateResponse(null, true), null, null);

    context.checking(new Expectations() {
      {
        oneOf(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
        will(returnFutureWithValue(response));
      }
    });

    hTable.put(Arrays.asList(new Put(row)));
  }

  @Test
  public void deleteCanSucceed()
      throws InterruptedException, ExecutionException, TimeoutException, IOException {
    Response response = new Response(Response.Command.MUTATE, 1l, null, new MutateResponse(null, true), null, null);

    context.checking(new Expectations() {
      {
        oneOf(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
        will(returnFutureWithValue(response));
      }
    });
    hTable.delete(new Delete(row));
  }

  @Test
  public void deletesCanSucceed()
      throws InterruptedException, ExecutionException, TimeoutException, IOException {
    Response response = new Response(Response.Command.MUTATE, 1l, null, new MutateResponse(null, true), null, null);

    context.checking(new Expectations() {
      {
        oneOf(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
        will(returnFutureWithValue(response));
      }
    });

    hTable.delete(Arrays.asList(new Delete(row)));
  }


  @Test(expected = IOException.class)
  public void getShouldErrorWithNullResponse() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    context.checking(new Expectations() {
      {
        oneOf(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
        will(returnFutureWithValue(new Response()));
      }
    });
    hTable.get(new Get(row));
  }

  @Test
  public void canScan() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    C5ClientScanner c5ClientScanner = context.mock(C5ClientScanner.class);

    ArrayList<c5db.client.generated.Result> results = new ArrayList<>();

    ByteBuffer cf = ByteBuffer.wrap(Bytes.toBytes("cf"));
    ByteBuffer cq = ByteBuffer.wrap(Bytes.toBytes("cq"));
    ByteBuffer value = ByteBuffer.wrap(Bytes.toBytes("value"));
    for (int i = 0; i != 10000; i++) {
      ByteBuffer row = ByteBuffer.wrap(Bytes.toBytes(i));
      results.add(new c5db.client.generated.Result(Arrays.asList(new Cell(row, cf, cq, 0l, CellType.PUT, value)), 1, true));
    }


    SettableFuture<C5ClientScanner> scanFuture = SettableFuture.create();
    context.checking(new Expectations() {
      {
        allowing(channel).writeAndFlush(with(any(Call.class)));
        oneOf(messageHandler).callScan(with(any(Call.class)), with(any((Channel.class))));
        will(returnValue(scanFuture));
      }
    });
    scanFuture.set(c5ClientScanner);
    context.checking(new Expectations() {
      {
        oneOf(c5ClientScanner).next();
        will(returnValue(results.get(0)));
      }
    });

    ResultScanner scanner = hTable.getScanner(Bytes.toBytes("cf"));
    Result result = scanner.next();
    ResultMatcher resultMatchers = new ResultMatcher(ProtobufUtil.toResult(results.get(0)));
    assertThat(result, resultMatchers);

    List<c5db.client.generated.Result> aHundredResults = results.subList(1, 101);
    context.checking(new Expectations() {
      {
        oneOf(c5ClientScanner).next(100);
        will(returnValue(aHundredResults.stream().toArray(c5db.client.generated.Result[]::new)));
      }
    });
    assertThat(c5ClientScanner.next(100).length, is(100));
    context.checking(new Expectations() {
      {
        oneOf(c5ClientScanner).close();
      }
    });
    scanner.close();
  }

  @Test
  public void canMutateRow() throws IOException {
    context.checking(new Expectations() {
      {
        oneOf(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
        will(returnFutureWithValue(new Response()));
      }
    });

    RowMutations rm = new RowMutations(Bytes.toBytes("row"));
    hTable.mutateRow(rm);
  }

  @Test
  public void canCheckAndPut() throws IOException {
    Response response = new Response(Response.Command.MUTATE, 1l, null, new MutateResponse(null, true), null, null);

    context.checking(new Expectations() {
      {
        oneOf(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
        will(returnFutureWithValue(response));
      }
    });
    hTable.checkAndPut(row, cf, cq, value, new Put(row));
  }

  @Test
  public void canCheckAndDelete() throws IOException {
    Response response = new Response(Response.Command.MUTATE, 1l, null, new MutateResponse(null, true), null, null);

    context.checking(new Expectations() {
      {
        oneOf(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
        will(returnFutureWithValue(response));
      }
    });
    hTable.checkAndDelete(row, cf, cq, value, new Delete(row));
  }

  @Test
  public void putsClearIfWeHaveAnError()
      throws InterruptedException, ExecutionException, TimeoutException, IOException {
    hTable.setAutoFlush(false, true);

    try {
      hTable.setWriteBufferSize(200);
      long messagesToPut = hTable.getWriteBufferSize();
      ArrayBlockingQueue<SettableFuture<Response>> futures = new ArrayBlockingQueue<>((int) messagesToPut);

      for (int i = 0; i != messagesToPut; i++) {
        SettableFuture<Response> response = SettableFuture.create();
        context.checking(new Expectations() {
          {
            allowing(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
            will(returnValue(response));
          }
        });
        Put put = new Put(new byte[1]);
        hTable.put(put);
        futures.add(response);
      }

      futures.remove().setException(new IOException("foo"));

      // Should only complete if queue is cleared by errors
      for (int i = 0; i != messagesToPut; i++) {
        Put put = new Put(new byte[1]);
        hTable.put(put);
      }


    } finally {
      hTable.setAutoFlush(true);
    }
  }

  @Test(expected = TimeoutException.class)
  public void putsDoNotClearIfWeHaveAnErrorIfClearOnErrorIsOff()
      throws InterruptedException, ExecutionException, TimeoutException, IOException {
    hTable.setAutoFlush(false, false);

    try {
      hTable.setWriteBufferSize(200);
      long messagesToPut = hTable.getWriteBufferSize();
      ArrayBlockingQueue<SettableFuture<Response>> futures = new ArrayBlockingQueue<>((int) messagesToPut);

      for (int i = 0; i != messagesToPut; i++) {
        SettableFuture<Response> response = SettableFuture.create();
        context.checking(new Expectations() {
          {
            allowing(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
            will(returnValue(response));
          }
        });
        Put put = new Put(new byte[1]);
        hTable.put(put);
        futures.add(response);
      }

      futures.remove().setException(new IOException("foo"));


      executorService.submit(() -> {
        for (int i = 0; i != messagesToPut; i++) {
          Put put = new Put(new byte[1]);
          try {
            hTable.put(put);
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }).get(1, TimeUnit.SECONDS);


    } finally {
      hTable.setAutoFlush(true);
    }
  }


}