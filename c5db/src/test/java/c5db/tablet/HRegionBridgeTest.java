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
package c5db.tablet;

import c5db.client.FakeHTable;
import c5db.client.ProtobufUtil;
import c5db.client.generated.Action;
import c5db.client.generated.ByteArrayComparable;
import c5db.client.generated.CompareType;
import c5db.client.generated.Condition;
import c5db.client.generated.Get;
import c5db.client.generated.MultiRequest;
import c5db.client.generated.MutateRequest;
import c5db.client.generated.MutationProto;
import c5db.client.generated.RegionAction;
import c5db.client.generated.RegionSpecifier;
import c5db.client.generated.Scan;
import c5db.client.generated.ScanRequest;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.regionserver.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import javax.management.Query;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static javax.management.Query.not;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.*;

public class HRegionBridgeTest {
  @Rule
  public JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(new Synchroniser());
  }};

  HRegionInterface hRegionInterface = context.mock(HRegionInterface.class);
  HRegionBridge hRegionBridge = new HRegionBridge(hRegionInterface);

  @Test
  public void testMutate() throws Exception {
    MutationProto mutation = ProtobufUtil.toMutation(MutationProto.MutationType.PUT, new Put(Bytes.toBytes("fakeRow")));
    context.checking(new Expectations() {
      {
        oneOf(hRegionInterface).put(with(any(Put.class)));
      }});

     hRegionBridge.mutate(mutation, new Condition());
   }

  @Test
  public void testDeleteWithFailCondition() throws Exception {
    MutationProto mutation = ProtobufUtil.toMutation(MutationProto.MutationType.DELETE, new Delete(Bytes.toBytes("fakeRow")));
    context.checking(new Expectations() {
      {
        oneOf(hRegionInterface).checkAndMutate(with(any(byte[].class)),
            with(any(byte[].class)),
            with(any(byte[].class)),
            with(any(CompareFilter.CompareOp.class)),
            with(any(org.apache.hadoop.hbase.filter.ByteArrayComparable.class)),
            with(any(Mutation.class)),
            with(any(boolean.class)));
        will(returnValue(false));
      }});

    Condition condition = new Condition(ByteBuffer.wrap(Bytes.toBytes("row")),
        ByteBuffer.wrap(Bytes.toBytes("cf")),
        ByteBuffer.wrap(Bytes.toBytes("cq")),
        CompareType.EQUAL,
        FakeHTable.toComparator(new ByteArrayComparable(ByteBuffer.wrap(Bytes.toBytes("value")))));

    assertThat(hRegionBridge.mutate(mutation, condition), is(false));

  }

  @Test
  public void testDeleteWithPassingCondition() throws Exception {

    MutationProto mutation = ProtobufUtil.toMutation(MutationProto.MutationType.DELETE, new Delete(Bytes.toBytes("fakeRow")));
    context.checking(new Expectations() {
      {
        oneOf(hRegionInterface).checkAndMutate(with(any(byte[].class)),
            with(any(byte[].class)),
            with(any(byte[].class)),
            with(any(CompareFilter.CompareOp.class)),
            with(any(org.apache.hadoop.hbase.filter.ByteArrayComparable.class)),
            with(any(Mutation.class)),
            with(any(boolean.class)));
        will(returnValue(true));
      }});

    Condition condition = new Condition(ByteBuffer.wrap(Bytes.toBytes("row")),
        ByteBuffer.wrap(Bytes.toBytes("cf")),
        ByteBuffer.wrap(Bytes.toBytes("cq")),
        CompareType.EQUAL,
        FakeHTable.toComparator(new ByteArrayComparable(ByteBuffer.wrap(Bytes.toBytes("value")))));

    assertThat(hRegionBridge.mutate(mutation, condition), is(true));
  }


  @Test
  public void testPutWithFailCondition() throws Exception {
    MutationProto mutation = ProtobufUtil.toMutation(MutationProto.MutationType.PUT, new Put(Bytes.toBytes("fakeRow")));
    context.checking(new Expectations() {
      {
        oneOf(hRegionInterface).checkAndMutate(with(any(byte[].class)),
            with(any(byte[].class)),
            with(any(byte[].class)),
            with(any(CompareFilter.CompareOp.class)),
            with(any(org.apache.hadoop.hbase.filter.ByteArrayComparable.class)),
            with(any(Mutation.class)),
            with(any(boolean.class)));
        will(returnValue(false));
      }});

    Condition condition = new Condition(ByteBuffer.wrap(Bytes.toBytes("row")),
        ByteBuffer.wrap(Bytes.toBytes("cf")),
        ByteBuffer.wrap(Bytes.toBytes("cq")),
        CompareType.EQUAL,
        FakeHTable.toComparator(new ByteArrayComparable(ByteBuffer.wrap(Bytes.toBytes("value")))));

    assertThat(hRegionBridge.mutate(mutation, condition), is(false));

  }

  @Test
  public void testPutWithPassingCondition() throws Exception {

    MutationProto mutation = ProtobufUtil.toMutation(MutationProto.MutationType.PUT, new Put(Bytes.toBytes("fakeRow")));
    context.checking(new Expectations() {
      {
        oneOf(hRegionInterface).checkAndMutate(with(any(byte[].class)),
            with(any(byte[].class)),
            with(any(byte[].class)),
            with(any(CompareFilter.CompareOp.class)),
            with(any(org.apache.hadoop.hbase.filter.ByteArrayComparable.class)),
            with(any(Mutation.class)),
            with(any(boolean.class)));
        will(returnValue(true));
      }});

    Condition condition = new Condition(ByteBuffer.wrap(Bytes.toBytes("row")),
        ByteBuffer.wrap(Bytes.toBytes("cf")),
        ByteBuffer.wrap(Bytes.toBytes("cq")),
        CompareType.EQUAL,
        FakeHTable.toComparator(new ByteArrayComparable(ByteBuffer.wrap(Bytes.toBytes("value")))));

    assertThat(hRegionBridge.mutate(mutation, condition), is(true));
  }

  @Test
   public void testGetTheRegion() throws Exception {
    hRegionBridge.getTheRegion();
   }

   @Test
   public void testExistsTrue() throws Exception {
     Result result = Result.create(new ArrayList<>());
     result.setExists(true);
     context.checking(new Expectations() {
       {
         oneOf(hRegionInterface).get(with(any(org.apache.hadoop.hbase.client.Get.class)));
         will(returnValue(result));
       }});
     Get get = ProtobufUtil.toGet(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("fakeRow")), true);
     assertThat(hRegionBridge.exists(get), is(true));
   }

  @Test
  public void testExistsFalse() throws Exception {
    Result result = Result.create(new ArrayList<>());
    result.setExists(false);
    context.checking(new Expectations() {
      {
        oneOf(hRegionInterface).get(with(any(org.apache.hadoop.hbase.client.Get.class)));
        will(returnValue(result));
      }});
    Get get = ProtobufUtil.toGet(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("fakeRow")), true);
    assertThat(hRegionBridge.exists(get), is(false));
  }

   @Test
   public void testGet() throws Exception {
     Result result = Result.create(new ArrayList<>());
     result.setExists(true);
     context.checking(new Expectations() {
       {
         oneOf(hRegionInterface).get(with(any(org.apache.hadoop.hbase.client.Get.class)));
         will(returnValue(result));
       }});
     Get get = ProtobufUtil.toGet(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("fakeRow")), true);
     hRegionBridge.get(get);

   }

   @Test(expected = IOException.class)
   public void testInvalidMulti() throws Exception {
     ByteBuffer regionLocation = ByteBuffer.wrap(Bytes.toBytes("testTable"));
     RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
         regionLocation);

     MutationProto mutation = ProtobufUtil.toMutation(MutationProto.MutationType.PUT, new Put(Bytes.toBytes("fakeRow")));
     Get get = ProtobufUtil.toGet(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("fakeRow")), false);

     List<RegionAction> regionActionList = new ArrayList<>();
     regionActionList.add(new RegionAction(regionSpecifier, true, Arrays.asList(new Action(0, mutation, null))));
     regionActionList.add(new RegionAction(regionSpecifier, true, Arrays.asList(new Action(1, mutation, null))));
     regionActionList.add(new RegionAction(regionSpecifier, true, Arrays.asList(new Action(2, null, get))));
     regionActionList.add(new RegionAction(regionSpecifier, true, Arrays.asList(new Action(3, mutation, get))));


     MultiRequest multiRequest = new MultiRequest(regionActionList);

     hRegionBridge.multi(multiRequest);
   }

  @Ignore
  @Test
  public void testMulti() throws Exception {
    ByteBuffer regionLocation = ByteBuffer.wrap(Bytes.toBytes("testTable"));
    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        regionLocation);

    MutationProto mutation = ProtobufUtil.toMutation(MutationProto.MutationType.PUT, new Put(Bytes.toBytes("fakeRow")));
    Get get = ProtobufUtil.toGet(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("fakeRow")), false);

    List<RegionAction> regionActionList = new ArrayList<>();
    regionActionList.add(new RegionAction(regionSpecifier, true, Arrays.asList(new Action(0, mutation, null))));
    regionActionList.add(new RegionAction(regionSpecifier, true, Arrays.asList(new Action(1, mutation, null))));
    regionActionList.add(new RegionAction(regionSpecifier, true, Arrays.asList(new Action(2, mutation, get))));


    MultiRequest multiRequest = new MultiRequest(regionActionList);

    hRegionBridge.multi(multiRequest);
  }

   @Test
   public void testGetScanner() throws Exception {
     RegionScanner mockScanner = context.mock(RegionScanner.class);
     context.checking(new Expectations() {
       {
         oneOf(hRegionInterface).getScanner(with(any(org.apache.hadoop.hbase.client.Scan.class)));
         will(returnValue(mockScanner));
       }});

     hRegionBridge.getScanner(new Scan());
   }
 }