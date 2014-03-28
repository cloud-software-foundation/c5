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

package c5db.log;

import c5db.C5CommonTestUtil;
import c5db.replication.generated.LogEntry;
import c5db.util.ExceptionHandlingBatchExecutor;
import c5db.util.ThrowFiberExceptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.lang.ArrayUtils;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.core.BatchExecutor;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class OLogTest {
  private static Path testDirectory;
  private static final int TIMEOUT = 10; //seconds
  private LogFileManager logFileManager;
  private OLog log;
  private Random deterministicDataSequence = null; // Used as a pre-seeded data source for at least one test

  @Rule
  public ThrowFiberExceptions throwFiberExceptions = new ThrowFiberExceptions(this);

  private final PoolFiberFactory fiberFactory = new PoolFiberFactory(Executors.newCachedThreadPool());
  private final BatchExecutor batchExecutor = new ExceptionHandlingBatchExecutor(throwFiberExceptions);


  private static void assertSameEntry(LogEntry entry1, LogEntry entry2) {
    assertEquals(entry1.getIndex(), entry2.getIndex());
    assertEquals(entry1.getTerm(), entry2.getTerm());
    assertArrayEquals(entry1.getData().array(), entry2.getData().array());
  }

  private static void assertSameEntryLists(List<LogEntry> list1, List<LogEntry> list2) {
    assertEquals(list1.size(), list2.size());
    for (int i = 0; i != list1.size(); i++) {
      assertSameEntry(list1.get(i), list2.get(i));
    }
  }

  /**
   * Helper method which wraps assertSameEntryLists, to more cleanly deal with the case where the actual
   * list is returned by a future.
   *
   * @return a future indicating that the assertion has taken place.
   */
  private static ListenableFuture<Boolean> assertEqualsFuture(List<LogEntry> expected,
                                                              ListenableFuture<List<LogEntry>> actual) {
    return Futures.transform(actual, (List<LogEntry> result) -> {
      assertSameEntryLists(expected, result);
      return true;
    });
  }

  /**
   * Get a log entry, and wait for its future to complete, for the sake of brevity below
   */
  private static LogEntry syncGetEntry(OLog log, long index, String quorumId) throws Exception {
    return log.getLogEntry(index, quorumId).get(TIMEOUT, TimeUnit.SECONDS);
  }

  /**
   * Get a sequence of log entries, and wait for the future to complete, for the sake of brevity below
   */
  private static List<LogEntry> syncGetLogEntries(OLog log, long start, long end, String quorumId)
      throws Exception {
    return log.getLogEntries(start, end, quorumId).get(TIMEOUT, TimeUnit.SECONDS);
  }

  /**
   * Method to create a LogEntry, as a wrapper to insulate from proto* changes
   */
  private static LogEntry makeEntry(long index, long term, String stringData) {
    return new LogEntry(term, index, ByteBuffer.wrap(stringData.getBytes()));
  }

  private static <T> List<T> cherryPickIndexes(List<T> inputList, Set<Integer> indexes) {
    List<T> result = Lists.newArrayListWithExpectedSize(indexes.size());
    for (int i = 0; i < inputList.size(); i++) {
      if (indexes.contains(i)) {
        result.add(inputList.get(i));
      }
    }
    return result;
  }

  private static <T> void waitForAll(List<ListenableFuture<T>> futures) throws Exception {
    for (ListenableFuture<T> future : futures) {
      future.get(TIMEOUT, TimeUnit.SECONDS);
    }
  }

  Fiber syncCallFiber = fiberFactory.create(batchExecutor);
  MemoryChannel<Boolean> syncCallsMade = new MemoryChannel<>();

  /**
   * Return a doctored instance of LogFileManager for use by tests. A complete mock would be impractical, since
   * the log relies on this object to produce nontrivial side effects. However, this is brittle;
   * TODO give LogFileManager some kind of hook or facility to let the test observe file sync; or, have it use
   * TODO some kind of abstract Writer factory?
   *
   * @return Anonymous subclass of LogFileManager, overridden to return Writers that let us observe their sync().
   * @throws Exception
   */
  private LogFileManager makeLogFileManager() throws Exception {
    return new LogFileManager(testDirectory) {
      @Override
      public Writer getNewWriter() throws IOException {
        return new Writer(getLogFile()) {
          @Override
          public void sync() throws IOException {
            super.sync();
            syncCallsMade.publish(true);
          }
        };
      }
    };
  }

  private String getArbitraryStringData() {
    // Some variable-length data to use for tests;
    return String.valueOf(deterministicDataSequence.nextLong());
  }

  /**
   * Create a list of LogEntry from a given start index (inclusive) to a given end index (exclusive).
   */
  private List<LogEntry> makeEntryList(long start, long end) {
    List<LogEntry> entries = Lists.newArrayList();
    for (long index = start; index < end; index++) {
      entries.add(makeEntry(index, Math.floorDiv(index, 7) + 1, getArbitraryStringData()));
    }
    return entries;
  }

  /**
   * Execute a simple script against the log. The "script" is an array of numbers. Positive numbers indicate that
   * an entry should be logged with that index. Negative numbers indicate that a truncation operation should be
   * made back to index equal to the absolute value of the negative number. Term is increased after each truncation
   * operation. Data logged is derived from a sequence of pseudorandom numbers starting from a particular seed.
   * The entries that were logged are returned, not including truncations.
   * <p>
   * All operations (truncations and writes) result in futures, which are added to the passed list.
   */
  private List<LogEntry> runLogScript(List<Long> script, String quorumId, List<ListenableFuture<Boolean>> futures)
      throws Exception {
    long term = 1;
    List<LogEntry> entriesLogged = Lists.newArrayList();

    for (long scriptItem : script) {
      long index = Math.abs(scriptItem);
      if (scriptItem < 0) {
        futures.add(log.truncateLog(index, quorumId));
        term++;
      } else {
        String data = getArbitraryStringData();
        LogEntry entry = makeEntry(index, term, data);
        entriesLogged.add(entry);
        futures.add(log.logEntry(Lists.newArrayList(entry), quorumId));
      }
    }
    return entriesLogged;
  }

  @BeforeClass
  public static void setTestDirectory() {
    if (testDirectory == null) {
      testDirectory = (new C5CommonTestUtil()).getDataTestDir("olog");
    }
  }

  @Before
  public final void setUp() throws Exception {
    logFileManager = makeLogFileManager();
    logFileManager.moveLogsToArchive();
    log = new NioOLog(logFileManager, fiberFactory, batchExecutor);
    syncCallFiber.start();
    deterministicDataSequence = new Random(2231983);
  }

  @After
  public final void tearDown() throws Exception {
    log.close();
    logFileManager.moveLogsToArchive();
    syncCallFiber.dispose();
    deterministicDataSequence = null;
  }

  @Test
  public void testGetLogEntryEmptyLog() throws Exception {
    String quorumId = "quorum";
    assertNull(syncGetEntry(log, 1, quorumId));
    assertNull(syncGetEntry(log, 0, quorumId));
  }

  @Test
  public void testGetLogEntry() throws Exception {
    String quorumId = "quorum";
    String anotherQuorumId = "another quorum";

    List<LogEntry> entries = Lists.newArrayList(
        makeEntry(1, 1, "first"),
        makeEntry(2, 1, "second"),
        makeEntry(4, 2, "third"));
    log.logEntry(entries.subList(0, 1), anotherQuorumId);
    log.logEntry(entries.subList(1, 3), quorumId);

    assertNull(syncGetEntry(log, 1, quorumId));
    assertSameEntry(entries.get(0), syncGetEntry(log, 1, anotherQuorumId));

    assertNull(syncGetEntry(log, 2, anotherQuorumId));
    assertSameEntry(entries.get(1), syncGetEntry(log, 2, quorumId));

    assertNull(syncGetEntry(log, 3, quorumId));

    assertSameEntry(entries.get(2), syncGetEntry(log, 4, quorumId));
    assertNull(syncGetEntry(log, 4, anotherQuorumId));
  }

  @Test
  public void testGetLogEntries() throws Exception {
    // Within this test, assume the log has no holes and all entries are in a particular quorum
    final String quorumId = "yet another quorum";
    final List<LogEntry> entries = Lists.newArrayList(
        makeEntry(1, 1, "first"),
        makeEntry(2, 1, "second"),
        makeEntry(3, 2, "third"),
        makeEntry(4, 2, "fourth"));

    log.logEntry(entries, quorumId);
    assertSameEntryLists(
        entries.subList(1, 4),
        syncGetLogEntries(log, 2, 6, quorumId));

    assertSameEntryLists(
        entries.subList(0, 2),
        syncGetLogEntries(log, 1, 3, quorumId));

    assertSameEntryLists(
        entries.subList(2, 3),
        syncGetLogEntries(log, 3, 4, quorumId));

    assertSameEntryLists(
        Lists.newArrayList(),
        syncGetLogEntries(log, 1, 1, quorumId));
  }

  @Test
  public void testQueuedWrites() throws Exception {
    final String quorumId = "q";
    long[] script = {1, 2, 3, -1, 1, 2, 3, 4, 5, 6, 7};
    List<ListenableFuture<Boolean>> scriptOperations = Lists.newArrayList();
    List<LogEntry> entriesLogged = runLogScript(
        Arrays.asList(ArrayUtils.toObject(script)), quorumId, scriptOperations);

    // Writes should appear to immediately take effect when checking term
    for (int i = 1; i <= 7; i++) {
      assertEquals(2, log.getLogTerm(i, quorumId));
    }

    assertSameEntry(entriesLogged.get(9), syncGetEntry(log, 7, quorumId));
    waitForAll(scriptOperations);
  }

  @Test
  public void testThatReadsWaitForTruncationsInProgress() throws Exception {
    final String firstQuorumId = "first quorum";
    final String secondQuorumId = "second quorum";

    List<LogEntry> firstEntries = makeEntryList(1, 500);
    List<LogEntry> secondEntries = makeEntryList(126, 130);

    log.logEntry(firstEntries, firstQuorumId);
    log.truncateLog(251, firstQuorumId);
    log.logEntry(secondEntries, secondQuorumId);

    // Even though the the first quorum has just received a large truncation request, this read should "see"
    // that the truncation has already taken place. In practice, that might be because the log is doing some
    // kind of in-memory caching of the unwritten truncation, or it might be that the log doesn't let this
    // read complete until the truncation is processed. This test doesn't care which possibility.
    assertSameEntryLists(firstEntries.subList(224, 250), syncGetLogEntries(log, 225, 275, firstQuorumId));

    // A second quorum requests and gets its data. The reason for including this in the test is to make sure
    // concurrent writes and reads to a second quorum aren't affected by the first quorum waiting.
    assertSameEntryLists(secondEntries, syncGetLogEntries(log, 126, 130, secondQuorumId));
  }

  @Test
  public void testMultipleOverlappingTruncationsWithinOneQuorum() throws Exception {
    final String quorumId = "q";
    long[] script = {1, 2, 3, 4, 5, 6, -4, 4, 5, -3, 3, 4, 5, 6, 7, 8, -6, 6, 7, -6, 6, 7, 8, 9, -8};

    List<ListenableFuture<Boolean>> scriptOperations = Lists.newArrayList();
    List<LogEntry> entriesLogged = runLogScript(
        Arrays.asList(ArrayUtils.toObject(script)), quorumId, scriptOperations);
    List<LogEntry> expectedEntries = cherryPickIndexes(entriesLogged, Sets.newHashSet(0, 1, 8, 9, 10, 16, 17));

    waitForAll(scriptOperations);

    List<ListenableFuture<Boolean>> asyncAssertions = ImmutableList.of(
        assertEqualsFuture(expectedEntries.subList(0, 7), log.getLogEntries(1, 12, quorumId)),
        assertEqualsFuture(expectedEntries.subList(1, 4), log.getLogEntries(2, 5, quorumId)),
        assertEqualsFuture(expectedEntries.subList(0, 2), log.getLogEntries(1, 3, quorumId)),
        assertEqualsFuture(expectedEntries.subList(3, 7), log.getLogEntries(4, 8, quorumId)),
        assertEqualsFuture(Lists.newArrayList(), log.getLogEntries(8, 10, quorumId)));

    for (LogEntry entry : expectedEntries) {
      assertEquals(entry.getTerm(), log.getLogTerm(entry.getIndex(), quorumId));
    }

    waitForAll(asyncAssertions);
  }

  @Test
  public void testGetLogEntriesWhenThereAreMultipleInterspersedQuorums() throws Exception {
    String testQuorumId = "the quorum being tested";
    String[] quorums = {"B", "C", "D", "E"};
    long[] script = {1, 2, 3, 4, 5, 6, -6, 6, 7, -6, -5, -4, -3, 3, 4, 5, 6, -6, 6, 7, 8};
    List<LogEntry> entriesLogged = Lists.newArrayList();
    List<ListenableFuture<Boolean>> scriptOperations = Lists.newArrayList();

    for (long scriptItem : script) {
      List<Long> itemAsList = Lists.newArrayList(scriptItem);
      entriesLogged.addAll(runLogScript(itemAsList, testQuorumId, scriptOperations));
      for (String quorumId : quorums) {
        runLogScript(itemAsList, quorumId, scriptOperations);
      }
    }

    List<LogEntry> expectedEntries = cherryPickIndexes(entriesLogged, Sets.newHashSet(0, 1, 8, 9, 10, 12, 13, 14));
    waitForAll(scriptOperations);

    for (LogEntry entry : expectedEntries) {
      assertEquals(entry.getTerm(), log.getLogTerm(entry.getIndex(), testQuorumId));
    }

    List<ListenableFuture<Boolean>> asyncAssertions = ImmutableList.of(
        assertEqualsFuture(expectedEntries.subList(0, 4), log.getLogEntries(1, 5, testQuorumId)),
        assertEqualsFuture(expectedEntries.subList(3, 8), log.getLogEntries(4, 9, testQuorumId)),
        assertEqualsFuture(expectedEntries.subList(0, 8), log.getLogEntries(1, 9, testQuorumId)));

    waitForAll(asyncAssertions);
  }

  @Test
  public void testSyncAfterMultipleSmallWrites() throws Exception {
    final int numEntriesInTest = 1000;
    final int numBytesInEntriesContent = 5;
    final String quorumId = "Q";
    final List<LogEntry> entriesLogged = Lists.newArrayList();

    // Write many small entries to the log
    for (long index = 1; index <= numEntriesInTest; index++) {
      final byte[] byteDataPerEntry = new byte[numBytesInEntriesContent];
      deterministicDataSequence.nextBytes(byteDataPerEntry);
      ByteBuffer data = ByteBuffer.wrap(byteDataPerEntry);

      LogEntry entry = new LogEntry(1, index, data);
      log.logEntry(Lists.newArrayList(entry), quorumId);
      entriesLogged.add(entry);
    }

    // This test affects log implementations that queue or batch their writes. Without the following sync,
    // it is highly unlikely (although theoretically possible) that this test will pass.
    log.sync().get(TIMEOUT, TimeUnit.SECONDS);

    // Simulate a shutdown of the log (does not sync, in and of itself)
    log.close();

    // Another log opens the same file (replace the previous one, so the @After method doesn't notice)
    log = new NioOLog(logFileManager, fiberFactory, batchExecutor);
    assertSameEntryLists(entriesLogged, syncGetLogEntries(log, 1, numEntriesInTest + 1, quorumId));
  }

  @Test
  public void testThatLogCallsUnderlyingSync() throws Exception {
    // It would be ideal to test logging a single large block of data to the log, then cause a catastrophic
    // interruption, such that if the log's sync() method had not been used, the lack of an underlying fsync
    // call would cause data loss. Such a hypothetical test would therefore use sync() before the interruption,
    // and so verify that all data was correctly persisted to disk. However, it is not simple to reliably
    // create such a catastrophic failure within the unit testing framework. So this test will simply verify
    // that the file descriptor's sync() method is called then the log's sync() method is.

    SettableFuture<Boolean> notificationThatSyncWasCalled = SettableFuture.create();
    log.sync();
    syncCallsMade.subscribe(syncCallFiber, notificationThatSyncWasCalled::set);
    notificationThatSyncWasCalled.get(2, TimeUnit.SECONDS);
  }
}
