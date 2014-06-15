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
import c5db.client.generated.MutationProto;
import c5db.client.generated.RegionAction;
import c5db.client.generated.RegionActionResult;
import c5db.client.generated.RegionSpecifier;
import c5db.client.generated.Scan;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.regionserver.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.MultiRowMutationProcessor;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.core.IsNull;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Rule;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class HRegionBridgeTest {
  @Rule
  public JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(new Synchroniser());
  }};

  private final HRegionInterface hRegionInterface = context.mock(HRegionInterface.class);
  private final HRegionBridge hRegionBridge = new HRegionBridge(hRegionInterface);

  @Test
  public void shouldBeAbleToMutate() throws Exception {
    MutationProto mutation = ProtobufUtil.toMutation(MutationProto.MutationType.PUT, new Put(Bytes.toBytes("fakeRow")));
    context.checking(new Expectations() {
      {
        oneOf(hRegionInterface).put(with(any(Put.class)));
      }
    });

    hRegionBridge.mutate(mutation, new Condition());
  }

  @Test
  public void shouldSkipDeleteWhenConditionDoesNotPass() throws Exception {
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
      }
    });

    Condition condition = new Condition(ByteBuffer.wrap(Bytes.toBytes("row")),
        ByteBuffer.wrap(Bytes.toBytes("cf")),
        ByteBuffer.wrap(Bytes.toBytes("cq")),
        CompareType.EQUAL,
        FakeHTable.toComparator(new ByteArrayComparable(ByteBuffer.wrap(Bytes.toBytes("value")))));

    assertThat(hRegionBridge.mutate(mutation, condition), is(false));

  }

  @Test
  public void shouldDeleteWhenConditionPasses() throws Exception {

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
      }
    });

    Condition condition = new Condition(ByteBuffer.wrap(Bytes.toBytes("row")),
        ByteBuffer.wrap(Bytes.toBytes("cf")),
        ByteBuffer.wrap(Bytes.toBytes("cq")),
        CompareType.EQUAL,
        FakeHTable.toComparator(new ByteArrayComparable(ByteBuffer.wrap(Bytes.toBytes("value")))));

    assertThat(hRegionBridge.mutate(mutation, condition), is(true));
  }


  @Test
  public void shouldSkipPutWhenConditionFails() throws Exception {
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
      }
    });

    Condition condition = new Condition(ByteBuffer.wrap(Bytes.toBytes("row")),
        ByteBuffer.wrap(Bytes.toBytes("cf")),
        ByteBuffer.wrap(Bytes.toBytes("cq")),
        CompareType.EQUAL,
        FakeHTable.toComparator(new ByteArrayComparable(ByteBuffer.wrap(Bytes.toBytes("value")))));

    assertThat(hRegionBridge.mutate(mutation, condition), is(false));

  }

  @Test
  public void shouldPutWhenConditionPasses() throws Exception {

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
      }
    });

    Condition condition = new Condition(ByteBuffer.wrap(Bytes.toBytes("row")),
        ByteBuffer.wrap(Bytes.toBytes("cf")),
        ByteBuffer.wrap(Bytes.toBytes("cq")),
        CompareType.EQUAL,
        FakeHTable.toComparator(new ByteArrayComparable(ByteBuffer.wrap(Bytes.toBytes("value")))));

    assertThat(hRegionBridge.mutate(mutation, condition), is(true));
  }

  @Test
  public void existShouldTrueWhenResultExists() throws Exception {
    Result result = Result.create(new ArrayList<>());
    result.setExists(true);
    context.checking(new Expectations() {
      {
        oneOf(hRegionInterface).get(with(any(org.apache.hadoop.hbase.client.Get.class)));
        will(returnValue(result));
      }
    });
    Get get = ProtobufUtil.toGet(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("fakeRow")), true);
    assertThat(hRegionBridge.exists(get), is(true));
  }

  @Test
  public void existShouldFalseWhenResultDoesNotExists() throws Exception {
    Result result = Result.create(new ArrayList<>());
    result.setExists(false);
    context.checking(new Expectations() {
      {
        oneOf(hRegionInterface).get(with(any(org.apache.hadoop.hbase.client.Get.class)));
        will(returnValue(result));
      }
    });
    Get get = ProtobufUtil.toGet(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("fakeRow")), true);
    assertThat(hRegionBridge.exists(get), is(false));
  }

  @Test
  public void shouldServeSimpleGet() throws Exception {
    Result result = Result.create(new ArrayList<>());
    result.setExists(true);
    context.checking(new Expectations() {
      {
        oneOf(hRegionInterface).get(with(any(org.apache.hadoop.hbase.client.Get.class)));
        will(returnValue(result));
      }
    });
    Get get = ProtobufUtil.toGet(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("fakeRow")), true);
    hRegionBridge.get(get);

  }

  @Test
  public void shouldEasilyDoSimpleAtomicMutationMulti() throws Exception {
    ByteBuffer regionLocation = ByteBuffer.wrap(Bytes.toBytes("testTable"));
    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        regionLocation);

    context.checking(new Expectations() {
      {
        oneOf(hRegionInterface).processRowsWithLocks(with(any(MultiRowMutationProcessor.class)));
      }
    });

    MutationProto mutation = ProtobufUtil.toMutation(MutationProto.MutationType.PUT, new Put(Bytes.toBytes("fakeRow")));

    RegionActionResult actions = hRegionBridge.processRegionAction(new RegionAction(regionSpecifier,
        true,
        Arrays.asList(new Action(0, mutation, null))));
    assertThat(actions.getException(), IsNull.nullValue());
    assertThat(actions.getResultOrExceptionList().size(), is(1));
  }


  @Test
  public void shouldEasilyDoAtomicMutationOnlyMultiPut() throws Exception {
    ByteBuffer regionLocation = ByteBuffer.wrap(Bytes.toBytes("testTable"));
    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        regionLocation);

    context.checking(new Expectations() {
      {
        oneOf(hRegionInterface).processRowsWithLocks(with(any(MultiRowMutationProcessor.class)));
      }
    });

    MutationProto mutation = ProtobufUtil.toMutation(MutationProto.MutationType.PUT, new Put(Bytes.toBytes("fakeRow")));

    RegionActionResult actions = hRegionBridge.processRegionAction(new RegionAction(regionSpecifier,
        true,
        Arrays.asList(
            new Action(0, mutation, null),
            new Action(1, mutation, null),
            new Action(2, mutation, null))
    ));
    assertThat(actions.getException(), IsNull.nullValue());
    assertThat(actions.getResultOrExceptionList().size(), is(3));
  }


  @Test
  public void shouldEasilyDoAtomicMutationOnlyMultiDelete() throws Exception {
    ByteBuffer regionLocation = ByteBuffer.wrap(Bytes.toBytes("testTable"));
    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        regionLocation);

    context.checking(new Expectations() {
      {
        oneOf(hRegionInterface).processRowsWithLocks(with(any(MultiRowMutationProcessor.class)));
      }
    });

    MutationProto mutation = ProtobufUtil.toMutation(MutationProto.MutationType.DELETE, new Delete(Bytes.toBytes("fakeRow")));

    RegionActionResult actions = hRegionBridge.processRegionAction(new RegionAction(regionSpecifier,
        true,
        Arrays.asList(
            new Action(0, mutation, null),
            new Action(1, mutation, null),
            new Action(2, mutation, null))
    ));
    assertThat(actions.getException(), IsNull.nullValue());
    assertThat(actions.getResultOrExceptionList().size(), is(3));
  }

  @Test
  public void shouldReturnExceptionAndNotAttemptMutationWhenWeAttemptAtomicMultiRowMutatePut() throws Exception {
    ByteBuffer regionLocation = ByteBuffer.wrap(Bytes.toBytes("testTable"));
    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        regionLocation);

    MutationProto mutation = ProtobufUtil.toMutation(MutationProto.MutationType.PUT, new Put(Bytes.toBytes("fakeRow")));
    MutationProto badMutation = ProtobufUtil.toMutation(MutationProto.MutationType.PUT, new Put(Bytes.toBytes("fakeRow2")));

    RegionActionResult actions = hRegionBridge.processRegionAction(new RegionAction(regionSpecifier,
        true,
        Arrays.asList(
            new Action(0, mutation, null),
            new Action(1, mutation, null),
            new Action(2, badMutation, null))
    ));
    assertThat(actions.getException(), IsNull.notNullValue());
    assertThat(actions.getResultOrExceptionList().size(), is(0));
  }

  @Test
  public void shouldReturnExceptionAndNotAttemptMutationWhenWeAttemptAtomicMultiRowMutateDelete() throws Exception {
    ByteBuffer regionLocation = ByteBuffer.wrap(Bytes.toBytes("testTable"));
    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        regionLocation);

    MutationProto mutation = ProtobufUtil.toMutation(MutationProto.MutationType.DELETE, new Delete(Bytes.toBytes("fakeRow")));
    MutationProto badMutation = ProtobufUtil.toMutation(MutationProto.MutationType.DELETE, new Delete(Bytes.toBytes("fakeRow2")));

    RegionActionResult actions = hRegionBridge.processRegionAction(new RegionAction(regionSpecifier,
        true,
        Arrays.asList(
            new Action(0, mutation, null),
            new Action(1, mutation, null),
            new Action(2, badMutation, null))
    ));
    assertThat(actions.getException(), IsNull.notNullValue());
    assertThat(actions.getResultOrExceptionList().size(), is(0));
  }

  @Test
  public void shouldFailWhenWeTryToAtomicMutateAndGet() throws Exception {
    ByteBuffer regionLocation = ByteBuffer.wrap(Bytes.toBytes("testTable"));
    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        regionLocation);

    MutationProto mutation = ProtobufUtil.toMutation(MutationProto.MutationType.DELETE, new Delete(Bytes.toBytes("fakeRow")));
    Get get = ProtobufUtil.toGet(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("fakeRow2")), false);
    Get exists = ProtobufUtil.toGet(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("fakeRow2")), true);

    RegionActionResult actions = hRegionBridge.processRegionAction(new RegionAction(regionSpecifier,
        true,
        Arrays.asList(
            new Action(0, mutation, get),
            new Action(1, mutation, exists))
    ));
    assertThat(actions.getException(), IsNull.notNullValue());
    assertThat(actions.getResultOrExceptionList().size(), is(0));
  }

  @Test
  public void shouldReturnGetsAndDoSimpleAtomicMultiMutation() throws Exception {
    ByteBuffer regionLocation = ByteBuffer.wrap(Bytes.toBytes("testTable"));
    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        regionLocation);


    MutationProto mutationPut = ProtobufUtil.toMutation(MutationProto.MutationType.PUT, new Put(Bytes.toBytes("fakeRow")));
    MutationProto mutationDelete = ProtobufUtil.toMutation(MutationProto.MutationType.DELETE, new Delete(Bytes.toBytes("fakeRow")));
    Get get = ProtobufUtil.toGet(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("fakeRow2")), false);
    Get exists = ProtobufUtil.toGet(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("fakeRow2")), true);
    Result result = Result.create(new ArrayList<>());
    result.setExists(true);

    context.checking(new Expectations() {
      {
        oneOf(hRegionInterface).processRowsWithLocks(with(any(MultiRowMutationProcessor.class)));
        exactly(2).of(hRegionInterface).get(with(any(org.apache.hadoop.hbase.client.Get.class)));
        will(returnValue(result));
      }
    });

    RegionActionResult actions = hRegionBridge.processRegionAction(new RegionAction(regionSpecifier,
        true,
        Arrays.asList(
            new Action(0, null, get),
            new Action(1, mutationPut, null),
            new Action(2, null, exists),
            new Action(3, mutationDelete, null))
    ));

    assertThat(actions.getException(), IsNull.nullValue());
    assertThat(actions.getResultOrExceptionList().size(), is(4));
  }


  @Test
  public void testGetScanner() throws Exception {
    RegionScanner mockScanner = context.mock(RegionScanner.class);
    context.checking(new Expectations() {
      {
        oneOf(hRegionInterface).getScanner(with(any(org.apache.hadoop.hbase.client.Scan.class)));
        will(returnValue(mockScanner));
      }
    });

    hRegionBridge.getScanner(new Scan());
  }

  @Test
  public void shouldEasilyDoSimpleMutationMulti() throws Exception {
    ByteBuffer regionLocation = ByteBuffer.wrap(Bytes.toBytes("testTable"));
    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        regionLocation);

    context.checking(new Expectations() {
      {
        oneOf(hRegionInterface).put(with(any(Put.class)));
      }
    });

    MutationProto mutation = ProtobufUtil.toMutation(MutationProto.MutationType.PUT, new Put(Bytes.toBytes("fakeRow")));

    RegionActionResult actions = hRegionBridge.processRegionAction(new RegionAction(regionSpecifier,
        false,
        Arrays.asList(new Action(0, mutation, null))));
    assertThat(actions.getException(), IsNull.nullValue());
    assertThat(actions.getResultOrExceptionList().size(), is(1));
  }


  @Test
  public void shouldEasilyDoMutationOnlyMultiPut() throws Exception {
    ByteBuffer regionLocation = ByteBuffer.wrap(Bytes.toBytes("testTable"));
    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        regionLocation);

    context.checking(new Expectations() {
      {
        exactly(3).of(hRegionInterface).put(with(any(Put.class)));
      }
    });

    MutationProto mutation = ProtobufUtil.toMutation(MutationProto.MutationType.PUT, new Put(Bytes.toBytes("fakeRow")));
    RegionActionResult actions = hRegionBridge.processRegionAction(new RegionAction(regionSpecifier,
        false,
        Arrays.asList(
            new Action(0, mutation, null),
            new Action(1, mutation, null),
            new Action(2, mutation, null))
    ));
    assertThat(actions.getException(), IsNull.nullValue());
    assertThat(actions.getResultOrExceptionList().size(), is(3));
  }


  @Test
  public void shouldEasilyDoMutationOnlyMultiDelete() throws Exception {
    ByteBuffer regionLocation = ByteBuffer.wrap(Bytes.toBytes("testTable"));
    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        regionLocation);

    context.checking(new Expectations() {
      {
        oneOf(hRegionInterface).delete(with(any(Delete.class)));
        oneOf(hRegionInterface).delete(with(any(Delete.class)));
        oneOf(hRegionInterface).delete(with(any(Delete.class)));
      }
    });

    MutationProto mutation = ProtobufUtil.toMutation(MutationProto.MutationType.DELETE, new Delete(Bytes.toBytes("fakeRow")));

    RegionActionResult actions = hRegionBridge.processRegionAction(new RegionAction(regionSpecifier,
        false,
        Arrays.asList(
            new Action(0, mutation, null),
            new Action(1, mutation, null),
            new Action(2, mutation, null))
    ));
    assertThat(actions.getException(), IsNull.nullValue());
    assertThat(actions.getResultOrExceptionList().size(), is(3));
  }

  @Test
  public void shouldBeAbleToProcessMultiRowNonAtomicPut() throws Exception {
    ByteBuffer regionLocation = ByteBuffer.wrap(Bytes.toBytes("testTable"));
    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        regionLocation);

    MutationProto mutation = ProtobufUtil.toMutation(MutationProto.MutationType.PUT, new Put(Bytes.toBytes("fakeRow")));
    MutationProto mutation2 = ProtobufUtil.toMutation(MutationProto.MutationType.PUT, new Put(Bytes.toBytes("fakeRow2")));

    context.checking(new Expectations() {
      {
        exactly(3).of(hRegionInterface).put(with(any(Put.class)));

      }
    });

    RegionActionResult actions = hRegionBridge.processRegionAction(new RegionAction(regionSpecifier,
        false,
        Arrays.asList(
            new Action(0, mutation, null),
            new Action(1, mutation, null),
            new Action(2, mutation2, null))
    ));
    assertThat(actions.getException(), IsNull.nullValue());
    assertThat(actions.getResultOrExceptionList().size(), is(3));
  }

  @Test
  public void shouldBeAbleToProcessMultiRowNonAtomicDelete() throws Exception {
    ByteBuffer regionLocation = ByteBuffer.wrap(Bytes.toBytes("testTable"));
    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        regionLocation);

    MutationProto mutation = ProtobufUtil.toMutation(MutationProto.MutationType.DELETE, new Delete(Bytes.toBytes("fakeRow")));
    MutationProto mutation2 = ProtobufUtil.toMutation(MutationProto.MutationType.DELETE, new Delete(Bytes.toBytes("fakeRow2")));


    context.checking(new Expectations() {
      {
        exactly(3).of(hRegionInterface).delete(with(any(Delete.class)));

      }
    });

    RegionActionResult actions = hRegionBridge.processRegionAction(new RegionAction(regionSpecifier,
        false,
        Arrays.asList(
            new Action(0, mutation, null),
            new Action(1, mutation, null),
            new Action(2, mutation2, null))
    ));
    assertThat(actions.getException(), IsNull.nullValue());
    assertThat(actions.getResultOrExceptionList().size(), is(3));
  }

  @Test
  public void shouldFailWhenWeTryToMutateAndGet() throws Exception {
    ByteBuffer regionLocation = ByteBuffer.wrap(Bytes.toBytes("testTable"));
    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        regionLocation);

    MutationProto mutation = ProtobufUtil.toMutation(MutationProto.MutationType.DELETE, new Delete(Bytes.toBytes("fakeRow")));
    Get get = ProtobufUtil.toGet(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("fakeRow2")), false);
    Get exists = ProtobufUtil.toGet(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("fakeRow2")), true);

    RegionActionResult actions = hRegionBridge.processRegionAction(new RegionAction(regionSpecifier,
        true,
        Arrays.asList(
            new Action(0, mutation, get),
            new Action(1, mutation, exists))
    ));
    assertThat(actions.getException(), IsNull.notNullValue());
    assertThat(actions.getResultOrExceptionList().size(), is(0));
  }

  @Test
  public void shouldReturnGetsAndDoSimpleMultiMutation() throws Exception {
    ByteBuffer regionLocation = ByteBuffer.wrap(Bytes.toBytes("testTable"));
    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        regionLocation);

    MutationProto mutationPut = ProtobufUtil.toMutation(MutationProto.MutationType.PUT, new Put(Bytes.toBytes("fakeRow")));
    MutationProto mutationDelete = ProtobufUtil.toMutation(MutationProto.MutationType.DELETE, new Delete(Bytes.toBytes("fakeRow")));
    Get get = ProtobufUtil.toGet(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("fakeRow2")), false);
    Get exists = ProtobufUtil.toGet(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("fakeRow2")), true);
    Result result = Result.create(new ArrayList<>());
    result.setExists(true);

    context.checking(new Expectations() {
      {
        oneOf(hRegionInterface).delete(with(any(Delete.class)));
        oneOf(hRegionInterface).put(with(any(Put.class)));
        exactly(2).of(hRegionInterface).get(with(any(org.apache.hadoop.hbase.client.Get.class)));
        will(returnValue(result));
      }
    });

    RegionActionResult actions = hRegionBridge.processRegionAction(new RegionAction(regionSpecifier,
        false,
        Arrays.asList(
            new Action(0, null, get),
            new Action(1, mutationPut, null),
            new Action(2, null, exists),
            new Action(3, mutationDelete, null))
    ));

    assertThat(actions.getException(), IsNull.nullValue());
    assertThat(actions.getResultOrExceptionList().size(), is(4));
  }

}