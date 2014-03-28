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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Support in-memory indexing operations for getting the "term" at a given log index. An object of this class
 * must only be used from one thread. The current implementation requires this object to be notified for every
 * log insert; however, the object only actually stores information when the term changes.
 *
 * This structure doesn't know about quorums, so there needs to be a separate one of these for each quorum.
 */
public class LogTermMap {
  private static final Logger LOG = LoggerFactory.getLogger(LogTermMap.class);
  private final NavigableMap<Long, Long> map;

  public LogTermMap(NavigableMap<Long, Long> initMap) {
    map = initMap;
  }

  public LogTermMap() {
    this(new TreeMap<>());
  }

  /**
   * Accept an entry's (index, term) and possibly incorporate this information in the map. Index and term
   * refer to the concepts used by the consensus algorithm. The algorithm guarantees that term is nondecreasing.
   *
   * @param index Log index
   * @param term Log term
   */
  public void incorporate(long index, long term) {
    final long lastTerm;
    if (map.isEmpty()) {
      lastTerm = 0;
    } else {
      lastTerm = map.lastEntry().getValue();
    }

    if (term > lastTerm) {
      map.put(index, term);
    } else if (term < lastTerm) {
      LOG.error("Encountered a decreasing term, {}, where the last known term was {}", term, lastTerm);
      throw new IllegalArgumentException("Decreasing term number");
    }
  }

  /**
   * This method removes information from the map. It should be used when the log has truncated some entries.
   *
   * @param index Index to truncate back to, inclusive.
   */
  public void truncateToIndex(long index) {
    map.tailMap(index, true).clear();
  }

  /**
   * Get the log term for a specified index, or zero if there is no preceding index such that the term can be
   * inferred.
   *
   * @param index Log index
   * @return The log term at this index
   */
  public long getTermAtIndex(long index) {
    Map.Entry<Long, Long> entry = map.floorEntry(index);
    return entry == null? 0 : entry.getValue();
  }

}
