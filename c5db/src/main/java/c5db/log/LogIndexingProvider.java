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
import com.google.common.util.concurrent.ListenableFuture;

import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.function.LongFunction;

/**
 * Describes a structure that keeps track of how indexes are distributed over the byte positions of a log file.
 * It accepts information about entries' indexes, quorum IDs, and file positions, and uses that information to
 * provide estimates of file position for arbitrary requested indexes.
 *
 * TODO how will implementations of this interface notify others about IO exceptions?
 */
public interface LogIndexingProvider extends Consumer<WrittenEntry> {

  /**
   * Accept a log entry and its file position, and possibly incorporate this information into the map.
   *
   * @param entry Log entry together with written file position
   */
  @Override
  void accept(WrittenEntry entry);

  /**
   * Get the file position of the entry with the greatest index less than or equal to the passed 'index' (within
   * the passed quorum). In other words, if we want to use the information in this structure to read the entry at
   * 'index' from the log, the result of this method is the latest byte position we can begin reading from the file
   * without the risk that we overshoot the entry. It is an estimate of the location of index.
   * TODO currently the implementation makes no guarantee about how good an estimate this is
   *
   * @param index Log index
   * @param quorumId Quorum id
   * @return A lower bound for the starting file position of the requested index.
   */
  long getPositionLowerBound(long index, String quorumId);

  /**
   * Synchronize index with the log it's indexing, by persisting any pending writes.
   *
   * @param upToFileEndPos Accept any pending writes up to and including the entry ending at this
   *                                   file position.
   * @return Future that indicates all specified writes have been accepted, and the sync operation has completed.
   */
  ListenableFuture<Boolean> sync(long upToFileEndPos);

  /**
   * Open the index and give it a chance to catch up to the main log, while the caller waits.
   *
   * @param indexPath Path to the file the index should use to persist its data
   * @param sendMissingEntries Allow the index to sync itself up to the main log. The passed function will be
   *                           called exactly once. The implementation will pass the function the value of
   *                           the last file position the index knows about -- i.e., the ending file position
   *                           of the latest log entry it has accepted. The function will receive that long, and
   *                           return an iterable containing all entries the index needs in order to catch up to
   *                           the main log.
   */
  void openAndSync(Path indexPath, LongFunction<Iterable<WrittenEntry>> sendMissingEntries);
}
