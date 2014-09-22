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

package c5db.log;

import c5db.interfaces.replication.GeneralizedReplicator;
import c5db.interfaces.replication.ReplicateSubmissionInfo;
import com.google.common.util.concurrent.Futures;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
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
import java.util.List;
import java.util.UUID;

import static c5db.FutureActions.returnFutureWithValue;

public class OLogShimTest {

  @Rule
  public JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(new Synchroniser());
  }};

  private final GeneralizedReplicator replicator = context.mock(GeneralizedReplicator.class);
  @SuppressWarnings("deprecation")
  private final HTableDescriptor descriptor = new HTableDescriptor();
  @SuppressWarnings("deprecation")
  private final HRegionInfo info = new HRegionInfo();
  private final TableName tableName = descriptor.getTableName();

  private final Fiber oLogShimFiber = new ThreadFiber();

  private HLog hLog;

  @Before
  public void setOverallExpectationsAndCreateTestObject() {
    context.checking(new Expectations() {{
      allowing(replicator).isAvailableFuture();
      will(returnFutureWithValue(null));
    }});

    oLogShimFiber.start();
    hLog = new OLogShim(replicator);
  }

  @After
  public void disposeOfFiber() {
    oLogShimFiber.dispose();
  }

  @Test
  public void logsOneReplicationDatumPerSubmittedWALEdit() throws Exception {
    context.checking(new Expectations() {{
      oneOf(replicator).replicate(with(anyData()));
    }});

    hLog.appendNoSync(info, tableName, aWalEditWithMultipleKeyValues(), aClusterIdList(), currentTime(), descriptor);
  }

  @Test(expected = IOException.class, timeout = 3000)
  public void syncThrowsAnExceptionIfTheReplicatorIsUnableToReplicateTheData()
      throws Exception {

    havingAppendedAndReceivedResponse(hLog, aFailureResponseWithSeqNum(1));

    hLog.sync(); // exception
  }

  @Test(timeout = 3000)
  public void syncCanWaitForSeveralPrecedingLogAppends() throws Exception {

    havingAppendedAndReceivedResponse(hLog, aSuccessResponseWithSeqNum(1));
    havingAppendedAndReceivedResponse(hLog, aSuccessResponseWithSeqNum(2));
    havingAppendedAndReceivedResponse(hLog, aSuccessResponseWithSeqNum(3));

    hLog.sync();
  }


  private void havingAppendedAndReceivedResponse(HLog hLog, ReplicateSubmissionInfo submissionInfo) throws Exception {
    context.checking(new Expectations() {{
      allowing(replicator).replicate(with(anyData()));
      will(returnFutureWithValue(submissionInfo));
    }});

    hLog.appendNoSync(info, tableName, aWalEditWithMultipleKeyValues(), aClusterIdList(), currentTime(), descriptor);
  }

  private ReplicateSubmissionInfo aSuccessResponseWithSeqNum(long seqNum) {
    return new ReplicateSubmissionInfo(seqNum, Futures.immediateFuture(null));
  }

  private ReplicateSubmissionInfo aFailureResponseWithSeqNum(long seqNum) {
    return new ReplicateSubmissionInfo(seqNum, Futures.immediateFailedFuture(new IOException()));
  }

  private WALEdit aWalEditWithMultipleKeyValues() {
    WALEdit edit = new WALEdit();

    for (int i = 0; i < 3; i++) {
      byte[] row = new byte[4];
      byte[] family = new byte[1];
      byte[] qualifier = new byte[128];
      byte[] value = new byte[128];
      edit.add(new KeyValue(row, family, qualifier, value));
    }
    return edit;
  }

  private List<UUID> aClusterIdList() {
    return new ArrayList<>();
  }

  private long currentTime() {
    return System.currentTimeMillis();
  }

  private Matcher<List<ByteBuffer>> anyData() {
    return Matchers.instanceOf(List.class);
  }
}
