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

import c5db.generated.WrittenEntry;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.LongFunction;

import static java.util.Map.Entry;

/**
 * LogIndexingProvider implementation within memory using a NavigableMap for each quorum.
 */
public class LogIndexingMemoryMap implements LogIndexingProvider {
  private final long indexSegmentBits; // every 2^(this many) bytes, record the position of an entry,
  private final Map<String, NavigableMap<Long, Long>> maps = new HashMap<>();
  private long lastEndFilePos = 0; // Latest file position accepted into the index; i.e., the end of the last entry

  private long syncUpToFileEndPos = 0;
  private SettableFuture<Boolean> syncFuture = null;

  public LogIndexingMemoryMap(int indexSegmentBits) {
    this.indexSegmentBits = indexSegmentBits;
  }

  private NavigableMap<Long, Long> getMapForQuorum(String quorumId) {
    if (maps.containsKey(quorumId)) {
      return maps.get(quorumId);
    } else {
      NavigableMap<Long, Long> newMap = new TreeMap<>();
      maps.put(quorumId, newMap);
      return newMap;
    }
  }

  private void truncateToIndex(long index, String quorumId) {
    getMapForQuorum(quorumId).tailMap(index, true).clear();
  }

  private boolean shouldRecord(String quorumId, long filePos) {
    NavigableMap<Long, Long> map = getMapForQuorum(quorumId);
    if (map.isEmpty()) {
      return true;
    }
    long lastOffsetNumber = map.lastEntry().getValue() >> indexSegmentBits;
    long thisOffsetNumber = filePos >> indexSegmentBits;
    return thisOffsetNumber > lastOffsetNumber;
  }

  @Override
  public void accept(WrittenEntry written) {
    final long startFilePos = written.getStartFilePos();
    final long endFilePos = written.getEndFilePos();
    final long index = written.getSeqNum();
    final String quorumId = written.getQuorumId();

    if (written.getTruncation()) {
      truncateToIndex(index, quorumId);
      return;
    }

    assert startFilePos >= lastEndFilePos;
    assert endFilePos > lastEndFilePos;

    if (shouldRecord(quorumId, startFilePos)) {
      getMapForQuorum(quorumId).put(index, startFilePos);
    }

    lastEndFilePos = endFilePos;

    if (syncFuture != null && endFilePos >= syncUpToFileEndPos) {
      syncFuture.set(true);
      syncFuture = null;
    }
  }

  @Override
  public long getPositionLowerBound(long index, String quorumId) {
    Entry<Long, Long> entry = getMapForQuorum(quorumId).floorEntry(index);
    return entry == null ? 0 : entry.getValue();
  }

  @Override
  public ListenableFuture<Boolean> sync(long upToFileEndPos) {
    if (lastEndFilePos >= upToFileEndPos) {
      return Futures.immediateFuture(true);
    }
    SettableFuture<Boolean> syncCompleted = SettableFuture.create();
    syncFuture = syncCompleted;
    syncUpToFileEndPos = upToFileEndPos;
    return syncCompleted;
  }

  @Override
  public void openAndSync(Path indexPath, LongFunction<Iterable<WrittenEntry>> sendMissingEntries) {
    // ignore path
    sendMissingEntries.apply(lastEndFilePos).forEach(this::accept);
  }
}
