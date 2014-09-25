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

package c5db.regionserver;

import c5db.client.generated.Call;
import c5db.client.generated.RegionSpecifier;
import c5db.client.generated.Response;
import c5db.client.generated.Scan;
import c5db.client.generated.ScanRequest;
import c5db.tablet.Region;
import io.netty.channel.ChannelHandlerContext;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static c5db.regionserver.AddElementsActionReturnTrue.addElements;

public class ScanRunnableTest {
  private final RegionScanner regionScanner;
  @Rule
  public JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(new Synchroniser());
  }};
  private final ChannelHandlerContext ctx = context.mock(ChannelHandlerContext.class);
  private final Region region = context.mock(Region.class);
  private ScanRunnable scanRunnable;

  public ScanRunnableTest() {
    regionScanner = context.mock(RegionScanner.class);
  }

  @Before
  public void before() throws IOException {
    RegionSpecifier regionSpecifier = new RegionSpecifier();

    Scan scan = new Scan();
    long scannerId = 1000;
    int numberOfRows = 100;
    ScanRequest scanRequest = new ScanRequest(regionSpecifier, scan, scannerId, numberOfRows, false, 0);
    long commandId = 1000;
    Call call = new Call(Call.Command.SCAN, commandId, null, null, scanRequest, null);
    context.checking(new Expectations() {
      {
        oneOf(region).getScanner(with(any(Scan.class)));
        will(returnValue(regionScanner));

      }
    });

    scanRunnable = new ScanRunnable(ctx, call, scannerId, region);

  }

  @Test
  public void scannerCanDeliverASingleMessageOnlyOnce() throws InterruptedException, IOException {
    byte[] row = Bytes.toBytes("row");
    byte[] cf = Bytes.toBytes("cf");
    byte[] cq = Bytes.toBytes("cq");
    byte[] value = Bytes.toBytes("value");
    KeyValue keyValue = new KeyValue(row, cf, cq, value);

    context.checking(new Expectations() {
      {
        oneOf(regionScanner).nextRaw(with(any(List.class)));
        will(addElements(keyValue));
        oneOf(ctx).writeAndFlush(with(any(Response.class)));
      }
    });


    scanRunnable.onMessage(1);
  }


  @Test
  public void scannerCanDeliverWithMultipleOnMessageInvocation() throws InterruptedException, IOException {
    ArrayList<KeyValue> kvs = new ArrayList<>();
    for (int i = 0; i != 10000; i++) {
      byte[] row = Bytes.toBytes(i);
      byte[] cf = Bytes.toBytes(i);
      byte[] cq = Bytes.toBytes(i);
      byte[] value = Bytes.toBytes(i);
      KeyValue keyValue = new KeyValue(row, cf, cq, value);
      kvs.add(keyValue);
    }

    context.checking(new Expectations() {
      {
        exactly(3).of(regionScanner).nextRaw(with(any(List.class)));
        will(addElements(kvs.toArray()));
        oneOf(ctx).writeAndFlush(with(any(Response.class)));
      }
    });

    scanRunnable.onMessage(3);
  }


}
