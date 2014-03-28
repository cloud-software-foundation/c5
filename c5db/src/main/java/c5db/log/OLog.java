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

import c5db.replication.generated.LogEntry;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;


public interface OLog extends AutoCloseable {

  /**
   * Append the passed entries to the log.
   *
   * @param entries  Non-null list of zero or more entries.
   * @param quorumId Quorum id these entries should be logged under
   * @return Future indicating completion. Failure will be indicated by exception.
   */
  ListenableFuture<Boolean> logEntry(List<LogEntry> entries, String quorumId);

  /**
   * Asynchronously retrieve the entry at the given index in the given quorum.
   *
   * @param index    Index of entry to retrieve
   * @param quorumId Quorum id of entry to retrieve
   * @return Future containing the log entry upon completion, or null if not found.
   */
  ListenableFuture<LogEntry> getLogEntry(long index, String quorumId);

  /**
   * Asynchronously retrieve a range of entries from index start, inclusive, to index end, exclusive. Returns every
   * entry in the specified range. Any entries retrieved are guaranteed to have consecutive indices.
   *
   * @param start    First index in range
   * @param end      One beyond the last index in the desired range; must be greater than or equal to start. If this
   *                 equals start, a list of length zero will be retrieved.
   * @param quorumId Quorum id of entries to retrieve
   * @return Future containing a list of log entries upon completion.
   */
  ListenableFuture<List<LogEntry>> getLogEntries(long start, long end, String quorumId);

  /**
   * Logically delete entries from the tail of the log.
   *
   * @param entryIndex Delete entries back to, and including, this index,
   * @param quorumId   Quorum id within which to delete entries
   * @return Future indicating completion.
   */
  ListenableFuture<Boolean> truncateLog(long entryIndex, String quorumId);

  /**
   * Flush all pending writes to the physical medium.
   * TODO should this also be specified to sync with any indexing provider?
   *
   * @return Future indicating completion or exception.
   */
  ListenableFuture<Boolean> sync();

  /**
   * Retrieve the "term" (i.e., leader or election term) corresponding to the given pair (index, quorum)
   *
   * @param index    Log entry index
   * @param quorumId Log entry quorum
   * @return The term for this entry, or zero if not found.
   */
  long getLogTerm(long index, String quorumId);

  /**
   * Save off and close log file, and begin a new log file.
   *
   * @throws IOException
   * @throws ExecutionException
   * @throws InterruptedException
   */
  @SuppressWarnings("UnusedDeclaration")
  void roll() throws IOException, ExecutionException, InterruptedException;

  /**
   * Dispose of held resources. This method does not perform a sync() first -- the caller should sync if necessary.
   * The rationale for not performing a sync is to give the caller flexibility with whether or not to close
   * immediately, or asynchronously.
   *
   * @throws IOException If an error occurs closing a file stream.
   */
  void close() throws IOException;

}
