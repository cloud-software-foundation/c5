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

import c5db.generated.OLogImmutableHeader;
import c5db.generated.OLogMutableHeader;
import c5db.generated.WrittenEntry;
import c5db.replication.generated.LogEntry;
import c5db.util.C5Futures;
import c5db.util.CheckedConsumer;
import c5db.util.FiberOnly;
import com.dyuproject.protostuff.ProtobufException;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.core.BatchExecutor;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static c5db.generated.OLogMutableHeader.Type.TRUNCATION;
import static c5db.log.EntryEncoding.CrcError;
import static c5db.log.EntryEncoding.IterationInfo;
import static c5db.log.EntryEncoding.decodeAndCheckCrc;
import static c5db.log.EntryEncoding.encodeEntry;
import static c5db.log.EntryEncoding.encodeTruncationMarker;
import static c5db.log.NioOLog.WriteRequest.Type.FLUSH_QUEUE;
import static c5db.log.NioOLog.WriteRequest.Type.QUORUM_SYNC;
import static c5db.log.NioOLog.WriteRequest.Type.TRUNCATE;
import static c5db.log.NioOLog.WriteRequest.Type.WRITE;

/**
 * OLog using NIO.
 */
public final class NioOLog implements OLog {
  private static final Logger LOG = LoggerFactory.getLogger(NioOLog.class);

  private static final int SYNC_TIMEOUT = 30; // seconds
  private static final int READER_TIMEOUT = 15; // seconds
  private static final int WRITER_TIMEOUT = 30; // seconds
  private static final int INDEX_SEGMENT_BITS = 14;

  private final LogFileManager logFileManager;
  private final BatchExecutor batchExecutor;
  private final PoolFiberFactory fiberFactory;

  private final LogIndexingProvider indexingProvider = new LogIndexingMemoryMap(INDEX_SEGMENT_BITS);
  private final Fiber indexingFiber;
  private final MemoryChannel<WrittenEntry> indexingQueue = new MemoryChannel<>();

  private LogFileManager.Writer logWriter;

  /**
   * Class to hold information specific to a single quorum
   */
  private static class PerQuorum {
    final LogTermMap termMap = new LogTermMap();
    final Queue<WriteRequest> blockedWrites = new LinkedList<>();

    // truncationInProgress and lastSeqNum may be written to by two separate fibers/threads (the writer thread and
    // a truncation thread). However, the logic of writing and truncation is such that it is impossible for these
    // to interfere. During truncation, all writes are queued; and during a write, a truncation cannot begin.
    boolean truncationInProgress;
    long lastSeqNum = 0;

    // Prevent reads during truncations and vice versa.
    ReadWriteLock rwl = new ReentrantReadWriteLock(false);
  }

  private final Map<String, PerQuorum> quorumMap = new HashMap<>();

  private static class WriteResult {
    final long postFilePos;

    private WriteResult(long postFilePos) {
      this.postFilePos = postFilePos;
    }
  }

  /**
   * Class describing a single request to the log's write queue.
   */
  static final class WriteRequest {
    enum Type {
      WRITE,        // write the entries to the specified quorum
      TRUNCATE,     // truncate the specified quorum to the "index" of the first element in entries
      FLUSH_QUEUE,  // flush the writeFiber's in-memory WriteRequest queue
      QUORUM_SYNC   // complete all queued writes for the specified quorum, including truncations
    }

    final Type type;
    final List<LogEntry> entries;
    final String quorumId;
    final SettableFuture<WriteResult> future;

    private WriteRequest(Type type, String quorumId, List<LogEntry> entries, SettableFuture<WriteResult> future) {
      this.type = type;
      this.quorumId = quorumId;
      this.entries = entries;
      this.future = future;
    }

    @Override
    public String toString() {
      return "WriteRequest{" +
          ", type=" + type.toString() +
          ", quorumId=\"" + quorumId +
          "\", entries=" + entries +
          '}';
    }
  }

  private final Fiber writeFiber;
  private final MemoryChannel<WriteRequest> writeQueue = new MemoryChannel<>();

  public NioOLog(LogFileManager logFileManager,
                 PoolFiberFactory fiberFactory,
                 BatchExecutor batchExecutor) throws IOException {
    this.logFileManager = logFileManager;
    this.fiberFactory = fiberFactory;
    this.batchExecutor = batchExecutor;

    this.writeFiber = fiberFactory.create(batchExecutor);
    writeQueue.subscribe(writeFiber, this::handleWriteRequest);
    writeFiber.start();

    indexingFiber = fiberFactory.create(batchExecutor);
    indexingQueue.subscribe(indexingFiber, indexingProvider::accept);
    indexingFiber.start();

    logWriter = logFileManager.getNewWriter();
  }

  /**
   * Retrieve the information structure for the desired quorum, creating it if it does not yet exist.
   *
   * @param quorumId The quorum whose information should be retrieved
   * @return Information structure for the passed quorum.
   */
  private PerQuorum perQuorum(String quorumId) {
    if (quorumMap.containsKey(quorumId)) {
      return quorumMap.get(quorumId);
    } else {
      PerQuorum newInfoObject = new PerQuorum();
      quorumMap.put(quorumId, newInfoObject);
      return newInfoObject;
    }
  }

  /**
   * Run a task asynchronously, not on the caller's thread.
   *
   * @param run Runnable to execute.
   */
  private void execute(Runnable run) {
    final Fiber fiber = fiberFactory.create(batchExecutor);
    fiber.start();
    fiber.execute(run);
  }

  @Override
  public ListenableFuture<Boolean> logEntry(List<LogEntry> entries, String quorumId) {
    final LogTermMap termMap = perQuorum(quorumId).termMap;
    entries.forEach((LogEntry e) -> termMap.incorporate(e.getIndex(), e.getTerm()));

    final SettableFuture<WriteResult> future = SettableFuture.create();
    writeQueue.publish(new WriteRequest(WRITE, quorumId, entries, future));
    return Futures.transform(future, (WriteResult result) -> true);
  }

  /**
   * Process a write request. Write requests come in several types, so this method dispatches to other methods
   * as necessary to handle particular types.
   *
   * @param writeRequest Request to process.
   */
  @FiberOnly
  private void handleWriteRequest(final WriteRequest writeRequest) {
    if (writeRequest.type == FLUSH_QUEUE) {
      writeRequest.future.set(new WriteResult(0));
      return;
    }

    final String quorumId = writeRequest.quorumId;
    final PerQuorum quorumInfo = perQuorum(quorumId);

    quorumInfo.blockedWrites.add(writeRequest);

    while (!quorumInfo.truncationInProgress && !quorumInfo.blockedWrites.isEmpty()) {
      final WriteRequest nextRequest = quorumInfo.blockedWrites.poll();

      try {
        final long currentFilePos = logWriter.position();

        if (nextRequest.type == WRITE) {
          write0(nextRequest);

        } else if (nextRequest.type == QUORUM_SYNC) {
          nextRequest.future.set(new WriteResult(currentFilePos));

        } else if (nextRequest.type == TRUNCATE) {
          final long truncateToIndex = nextRequest.entries.get(0).getIndex();
          quorumInfo.truncationInProgress = true;
          execute(() -> doTruncation(truncateToIndex, quorumId, currentFilePos, nextRequest.future));
          indexingQueue.publish(new WrittenEntry(quorumId, truncateToIndex, 0, 0, true));
        }
      } catch (Throwable t) {
        LOG.error("Error encountered executing write request {}: {}", nextRequest, t);
        nextRequest.future.setException(t);
      }
    }
  }

  /**
   * Write entries to the log.
   *
   * @param writeRequest A write request containing one or more log entries.
   * @throws IOException
   */
  @FiberOnly
  private void write0(final WriteRequest writeRequest) throws IOException {
    assert writeRequest.type == WRITE;

    String quorumId = writeRequest.quorumId;

    for (LogEntry entry : writeRequest.entries) {
      final ByteBuffer[] serializedData = encodeEntry(entry, quorumId);

      final long entryStartFilePos = logWriter.position();
      logWriter.write(serializedData);
      final long entryEndFilePos = logWriter.position();

      perQuorum(quorumId).lastSeqNum = entry.getIndex();
      indexingQueue.publish(new WrittenEntry(
          quorumId, entry.getIndex(), entryStartFilePos, entryEndFilePos, false));
    }
    final long postFilePos = logWriter.position();
    writeRequest.future.set(new WriteResult(postFilePos));
  }

  /**
   * Perform a truncation of the log within the specified quorum. Truncation deletes entries from tail (most
   * recent) end of the log, going backwards until the sequence number specified, inclusive. Because the log can
   * contain several interspersed quorums, it's not generally possible just to truncate the physical file itself.
   * <p>
   * The strategy is to actually mark entries deleted by overwriting their header with a truncation marker, so
   * that future read operations know to disregard the entry. Writing starts at the latest entry in the specified
   * quorum, and proceeds backwards, with 'start' being the last entry overwritten.
   *
   * @param start       The sequence number of the log entry to truncate back to; that is, delete every entry in
   *                    this quorum where entry's seqNum >= start.
   * @param quorumId    The quorum ID.
   * @param skipForward A file position within this file, included in the truncation marker, as an instruction
   *                    for readers to skip forward to a later position in the file (i.e., to skip over the
   *                    section of deleted entries.
   * @param future      The future to notify that the truncation operation has completed.
   */
  @FiberOnly
  private void doTruncation(long start, String quorumId, long skipForward, SettableFuture<WriteResult> future) {
    final PerQuorum quorumInfo = perQuorum(quorumId);

    try (FileChannel channel = logFileManager.getNewIOChannel()) {
      final Stack<Long> filePosStack = new Stack<>();

      positionChannelForIO(channel, quorumId, start);
      iterateEntries(channel,
          start,
          quorumInfo.lastSeqNum + 1,
          quorumId,
          (iterationInfo) -> filePosStack.push(iterationInfo.mutableHeaderFilePos));

      final ByteBuffer[] truncationMarkerBufs = encodeTruncationMarker(skipForward);

      final boolean hasLock = quorumInfo.rwl.writeLock().tryLock(WRITER_TIMEOUT, TimeUnit.SECONDS);
      if (!hasLock) {
        LOG.error("Timeout while waiting for a lock to truncate quorum \"{}\" to index {}", quorumId, start);
        throw new TimeoutException("trylock timeout in doTruncation");
      }

      try {
        while (!filePosStack.isEmpty()) {
          final long filePos = filePosStack.pop();
          channel.position(filePos);
          channel.write(truncationMarkerBufs);
          for (ByteBuffer buf : truncationMarkerBufs) {
            buf.rewind();
          }
        }
        channel.force(true);
      } finally {
        quorumInfo.rwl.writeLock().unlock();
      }

    } catch (Throwable t) {
      LOG.error("Error encountered while performing a truncation to index {} on quorum {}: {}",
          start, quorumId, t);
      future.setException(t);
      return;
    }

    quorumInfo.lastSeqNum = start - 1;
    quorumInfo.truncationInProgress = false;
    future.set(new WriteResult(skipForward));
    writeQueue.publish(new WriteRequest(QUORUM_SYNC, quorumId, null, SettableFuture.create()));
  }

  @Override
  public ListenableFuture<LogEntry> getLogEntry(long index, String quorumId) {
    // Wrapper for the multi-entry case
    return Futures.transform(getLogEntries(index, index + 1, quorumId),
        (List<LogEntry> result) -> {
          if (result.isEmpty()) {
            return null;
          } else {
            return result.get(0);
          }
        });
  }

  @Override
  public ListenableFuture<List<LogEntry>> getLogEntries(long start, long end, String quorumId) {
    assert start <= end;
    if (start == end) {
      return Futures.immediateFuture(new ArrayList<>());
    }

    // TODO caching necessary/possible for recently-written entries?
    SettableFuture<List<LogEntry>> resultFuture = SettableFuture.create();
    execute(() -> {
      List<LogEntry> results = new ArrayList<>();

      try {
        // For the sake of implementation simplicity, sync right off the bat.
        // TODO it isn't necessarily needed to sync first; could we know whether a given index is on disk?
        // TODO Or, could we fetch directly from the write queue?
        quorumSync(quorumId).get(SYNC_TIMEOUT, TimeUnit.SECONDS);

        try (FileChannel channel = logFileManager.getNewInputChannel()) {
          positionChannelForIO(channel, quorumId, start);
          results = performScanningRead(channel, start, end, quorumId);
        }
      } catch (Throwable t) {
        // TODO currently a failure here will fail the entire replicator instance; that may not be necessary
        LOG.error("Error encountered reading log entries: quorum \"{}\", requested start {} end {} - {}",
            quorumId, start, end, t);
        resultFuture.setException(t);
        return;
      }

      resultFuture.set(results);
    });

    return resultFuture;
  }

  /**
   * Using the indexing provider, position the given channel so it is ready to read the entry with the
   * specified sequence number form the specified quorum.
   *
   * @param channel  FileChannel to position.
   * @param quorumId Quorum ID.
   * @param seqNum   Entry sequence number within the quorum.
   * @throws IOException
   */
  private void positionChannelForIO(FileChannel channel, String quorumId, long seqNum) throws IOException {
    long advanceToPos = indexingProvider.getPositionLowerBound(seqNum, quorumId);
    channel.position(advanceToPos);
  }

  /**
   * Perform the file read operation, over a specified index range of the log file, ignoring entries not in the
   * specified quorum. Return all log entries found within that index range and quorum.
   *
   * @param channel  A FileChannel, ready for reading, and already advanced to a suitable position -- on a
   *                 boundary between entries, and less than or equal to the position of the first entry in the
   *                 requested range.
   * @param start    Sequence number of the first entry to return.
   * @param end      One beyond the sequence number of the last entry to return. This method will keep reading
   *                 until it encounters the entry with seqNum == end - 1.
   * @param quorumId The quorum ID; ignore any entry not in this quorum.
   * @return The list of all entries found; or, an empty list if no entries are found.
   * @throws IOException           If an error is encountered while reading
   * @throws InterruptedException, TimeoutException If the method is interrupted or times out while attempting
   *                               to acquire a read lock
   */
  @FiberOnly
  private List<LogEntry> performScanningRead(FileChannel channel, long start, long end, String quorumId)
      throws IOException, InterruptedException, TimeoutException {
    final List<LogEntry> results = new ArrayList<>();

    iterateEntries(channel, start, end, quorumId, iterationInfo -> {
      results.add(new LogEntry(
          iterationInfo.getTerm(),
          iterationInfo.getSeqNum(),
          iterationInfo.getContent()));
    });

    return results;
  }

  /**
   * Iterate over non-deleted entries in the log, within a single quorum, executing a callback for each one
   * (synchronously, on the caller's thread).
   *
   * @param channel     A FileChannel, ready for reading, and already advanced to a suitable position -- on a
   *                    boundary between entries, and less than or equal to the position of the first entry in the
   *                    requested range.
   * @param start       Sequence number of the first entry to execute the callback for.
   * @param end         One beyond the sequence number of the last entry to return. This method will keep reading
   *                    until it encounters the entry with seqNum == end - 1.
   * @param quorumId    Quorum ID.
   * @param callForEach Execute this callback for each entry, passing it an IterationInfo object which describes
   *                    the current state of the iteration, and allows the callback to access the entry's
   *                    data.
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @FiberOnly
  private void iterateEntries(
      FileChannel channel,
      long start,
      long end,
      String quorumId,
      CheckedConsumer<IterationInfo, IOException> callForEach)
      throws IOException, InterruptedException, TimeoutException {

    final InputStream inputStream = Channels.newInputStream(channel);
    final PerQuorum quorumInfo = perQuorum(quorumId);

    final boolean hasLock = quorumInfo.rwl.readLock().tryLock(READER_TIMEOUT, TimeUnit.SECONDS);
    if (!hasLock) {
      LOG.warn("Timed out while waiting for a lock to read the log - " +
          "requested start {} end {} quorum \"{}\"", start, end, quorumId);
      throw new TimeoutException("tryLock timeout in iterateEntries()");
    }

    try {
      // TODO This loop is very tightly coupled to EntryEncoding; is there any way to abstract out the decoding logic?
      do {
        final OLogImmutableHeader immutableHeader = decodeAndCheckCrc(inputStream, OLogImmutableHeader.getSchema());
        final long index = immutableHeader.getSeqNum();
        final String entryQuorumId = immutableHeader.getQuorumId();
        final long positionOfNextEntry = channel.position() + immutableHeader.getRemainingLength();

        if (!entryQuorumId.equals(quorumId)) {
          channel.position(positionOfNextEntry);
          continue;
        }

        final OLogMutableHeader mutableHeader;
        final long mutableHeaderFilePos = channel.position();
        try {
          mutableHeader = decodeAndCheckCrc(inputStream, OLogMutableHeader.getSchema());
        } catch (ProtobufException | CrcError e) {
          // TODO How to handle this? Truncate for this quorum?
          channel.position(positionOfNextEntry);
          continue;
        }

        if (mutableHeader.getType() == TRUNCATION) {
          channel.position(mutableHeader.getSkipForward());
          continue;
        }

        if (index < start) {
          channel.position(positionOfNextEntry);
        } else if (index >= start && index < end) {
          // TODO Add assertions to ensure consecutive, ascending indices
          callForEach.accept(new IterationInfo(inputStream, immutableHeader, mutableHeader, mutableHeaderFilePos));
          channel.position(positionOfNextEntry); // TODO this may often be a no-op.
        } else {
          break;
        }

      } while (true);
    } catch (EOFException ignore) {
    } catch (ProtobufException e) {
      LOG.error("Deserialization error encountered during log read (quorum \"{}\" start {} end {}) - " +
          "current position is {} ", quorumId, start, end, channel.position());
      throw e;
    } finally {
      quorumInfo.rwl.readLock().unlock();
    }
  }

  @Override
  public ListenableFuture<Boolean> truncateLog(long entryIndex, String quorumId) {
    final SettableFuture<WriteResult> truncationComplete = SettableFuture.create();
    final LogEntry tombstone = new LogEntry(0, entryIndex, null);
    final WriteRequest truncationRequest = new WriteRequest(
        TRUNCATE, quorumId, Lists.newArrayList(tombstone), truncationComplete);

    writeQueue.publish(truncationRequest);
    perQuorum(quorumId).termMap.truncateToIndex(entryIndex);

    return Futures.transform(truncationComplete, (WriteResult result) -> true);
  }

  /**
   * Get confirmation from the indexing provider that it has caught up to a given file position
   *
   * @param upToFileEndPos Request the index to synchronize up to
   * @param syncFuture     After the sync completes, set this future to true (or set its exception)
   */
  private void syncIndexWithLog(long upToFileEndPos, SettableFuture<Boolean> syncFuture) {
    final ListenableFuture<Boolean> indexSynced = indexingProvider.sync(upToFileEndPos);
    C5Futures.addCallback(
        indexSynced,
        (bool) -> syncFuture.set(true),
        syncFuture::setException,
        indexingFiber);
  }

  /**
   * Wait for all WriteRequests to the specified quorum to complete. In other words, if a truncation or write
   * was requested prior to this method being called, this method guarantees that that it will have completed
   * when the returned future is done.
   *
   * @param quorumId The quorum ID to wait for.
   * @return A future that will complete after all pending writes and truncations to that quorum have completed.
   */
  private ListenableFuture<WriteResult> quorumSync(String quorumId) {
    final SettableFuture<WriteResult> syncComplete = SettableFuture.create();
    writeQueue.publish(new WriteRequest(QUORUM_SYNC, quorumId, null, syncComplete));
    return syncComplete;
  }

  @Override
  public ListenableFuture<Boolean> sync() {
    final SettableFuture<WriteResult> queueFlushed = SettableFuture.create();
    final SettableFuture<Boolean> syncComplete = SettableFuture.create();

    C5Futures.addCallback(
        queueFlushed,
        (WriteResult result) -> {
          if (syncComplete.isCancelled()) {
            return;
          }
          try {
            for (String quorum : quorumMap.keySet()) {
              if (quorumMap.get(quorum).truncationInProgress) {
                quorumSync(quorum).get(SYNC_TIMEOUT, TimeUnit.SECONDS);
              }
            }
            logWriter.sync();
            syncIndexWithLog(result.postFilePos, syncComplete);
          } catch (Throwable t) {
            LOG.error("Error encountered during sync(): {}", t);
            syncComplete.setException(t);
          }
        },
        syncComplete::setException,
        writeFiber);

    writeQueue.publish(new WriteRequest(FLUSH_QUEUE, null, null, queueFlushed));

    return syncComplete;
  }

  @Override
  public long getLogTerm(long index, String quorumId) {
    return perQuorum(quorumId).termMap.getTermAtIndex(index);
  }

  @Override
  public void roll() throws IOException, ExecutionException, InterruptedException {
    logWriter.close();
    quorumMap.clear();
    logFileManager.roll();
    logWriter = logFileManager.getNewWriter();
  }

  @Override
  public void close() throws IOException {
    writeFiber.dispose();
    indexingFiber.dispose();
    logWriter.close();
  }

}
