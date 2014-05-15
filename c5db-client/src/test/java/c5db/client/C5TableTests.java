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
import c5db.client.generated.MutateResponse;
import c5db.client.generated.Response;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.protostuff.ByteString;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class C5TableTests {


  @Rule
  public JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(new Synchroniser());
  }};

  MessageHandler messageHandler = context.mock(MessageHandler.class);
  ChannelPipeline channelPipeline = context.mock(ChannelPipeline.class);
  C5ConnectionManager c5ConnectionManager = context.mock(C5ConnectionManager.class);
  Channel channel = context.mock(Channel.class);
  C5AsyncDatabase c5AsyncDatabase;
  SettableFuture callFuture;
  private byte[] row = Bytes.toBytes("row");
  private FakeHTable hTable;

  @Before
  public void before() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    c5AsyncDatabase = new C5AsyncDatabase("fake", 0, c5ConnectionManager);
    hTable = new FakeHTable(c5AsyncDatabase, ByteString.copyFromUtf8("Doesntexist"));
    callFuture = SettableFuture.create();
  }

  @After
  public void after() throws InterruptedException {

    context.checking(new Expectations() {
      {
        oneOf(c5ConnectionManager).close();
      }
    });

    c5AsyncDatabase.close();
  }

  @Test(expected = IllegalStateException.class)
  public void putShouldErrorOnInvalidResponse() throws IOException, InterruptedException, ExecutionException, TimeoutException, MutationFailedException {
    context.checking(new Expectations() {
      {
        oneOf(c5ConnectionManager).getOrCreateChannel(with(any(String.class)), with(any(int.class)));
        will(returnValue(channel));

        oneOf(channel).pipeline();
        will(returnValue(channelPipeline));

        oneOf(channelPipeline).get(with(any(Class.class)));
        will(returnValue(messageHandler));

        oneOf(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
        will(returnValue(callFuture));
      }
    });
    callFuture.set(new Response());
    hTable.put(new Put(row));

  }

  @Test(expected = MutationFailedException.class)
  public void putShouldThrowErrorIfMutationFailed()
      throws InterruptedException, ExecutionException, TimeoutException, MutationFailedException, IOException {
    context.checking(new Expectations() {
      {
        oneOf(c5ConnectionManager).getOrCreateChannel(with(any(String.class)), with(any(int.class)));
        will(returnValue(channel));

        oneOf(channel).pipeline();
        will(returnValue(channelPipeline));

        oneOf(channelPipeline).get(with(any(Class.class)));
        will(returnValue(messageHandler));

        oneOf(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
        will(returnValue(callFuture));
      }
    });
    Response response = new Response(Response.Command.MUTATE, 1l, null, new MutateResponse(null, false), null, null);
    callFuture.set(response);
    hTable.put(new Put(row));
  }

  public void putCanSucceed()
      throws InterruptedException, ExecutionException, TimeoutException, MutationFailedException, IOException {
    context.checking(new Expectations() {
      {
        oneOf(c5ConnectionManager).getOrCreateChannel(with(any(String.class)), with(any(int.class)));
        will(returnValue(channel));

        oneOf(channel).pipeline();
        will(returnValue(channelPipeline));

        oneOf(channelPipeline).get(with(any(Class.class)));
        will(returnValue(messageHandler));

        oneOf(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
        will(returnValue(callFuture));
      }
    });
    Response response = new Response(Response.Command.MUTATE, 1l, null, new MutateResponse(null, true), null, null);
    callFuture.set(response);
    hTable.put(new Put(row));
  }

  public void getShouldErrorWithNullResponse() throws IOException, InterruptedException, ExecutionException, TimeoutException{
    context.checking(new Expectations() {
      {
        oneOf(c5ConnectionManager).getOrCreateChannel(with(any(String.class)), with(any(int.class)));
        will(returnValue(channel));

        oneOf(channel).pipeline();
        will(returnValue(channelPipeline));

        oneOf(channelPipeline).get(with(any(Class.class)));
        will(returnValue(messageHandler));

        oneOf(messageHandler).call(with(any(Call.class)), with(any((Channel.class))));
        will(returnValue(callFuture));
      }
    });
    callFuture.set(new Response());
    hTable.get(new Get(row));
  }


}
