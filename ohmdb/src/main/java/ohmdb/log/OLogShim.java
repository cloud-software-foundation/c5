/*
 * Copyright (C) 2013  Ohm Data
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
package ohmdb.log;

import com.google.protobuf.ByteString;
import ohmdb.generated.Log;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.exceptions.FailedLogCloseException;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class OLogShim extends OLog implements Syncable, HLog {

  private final AtomicLong logSeqNum = new AtomicLong(0);
  private final UUID uuid;

  public OLogShim(String basePath) throws IOException {
    super(basePath);
    this.uuid = UUID.randomUUID();

  }

  //TODO fix so we don't always insert a huge amount of data
  @Override
  public Long startCacheFlush(byte[] encodedRegionName) {
    return (long) 0;
  }

  @Override
  public void completeCacheFlush(byte[] encodedRegionName) {
    LOG.error("completeCache");
  }

  @Override
  public void abortCacheFlush(byte[] encodedRegionName) {
    LOG.error("abort");
  }

  @Override
  public boolean isLowReplicationRollEnabled() {
    return false;
  }

  @Override
  public long getEarliestMemstoreSeqNum(byte[] encodedRegionName) {
    return 0;
  }

  @Override
  public void registerWALActionsListener(WALActionsListener listener) {
    LOG.error("reg");

  }

  @Override
  public boolean unregisterWALActionsListener(WALActionsListener listener) {
    return false;
  }

  @Override
  public WALCoprocessorHost getCoprocessorHost() {
    return null;
  }


  @Override
  public byte[][] rollWriter() throws IOException {
    roll();
    return null;
  }

  @Override
  public byte[][] rollWriter(boolean force)
      throws FailedLogCloseException, IOException {
    return rollWriter();
  }


  public void closeAndDelete() throws IOException {
    close();
    boolean success = this.logPath.toFile().delete();
    if (!success) {
      throw new IOException("Unable to close and delete: "
          + this.logPath.toFile().toString());
    }
  }

  @Override
  public void hflush() throws IOException {
    this.sync();
  }

  @Override
  public void hsync() throws IOException {
    this.sync();
  }

  @Override
  public void sync(long txid) throws IOException {
    this.sync();
  }

  @Override
  public void setSequenceNumber(long newValue) {
    for (long id = this.logSeqNum.get(); id < newValue &&
        !this.logSeqNum.compareAndSet(id, newValue); id = this.logSeqNum.get()) {
      LOG.debug("Changed sequenceid from " + id + " to " + newValue);
    }
  }

  @Override
  public long getSequenceNumber() {
    return logSeqNum.get();

  }

  @Override
  public long obtainSeqNum() {
    return this.logSeqNum.incrementAndGet();
  }


  @Override
  public void append(HRegionInfo info,
                     byte[] tableName,
                     WALEdit edits,
                     long now,
                     HTableDescriptor htd) throws IOException {
    this.append(info, tableName, edits, uuid, now, htd);
  }


  @Override
  public long appendNoSync(HRegionInfo info,
                           byte[] tableName,
                           WALEdit edits,
                           UUID clusterId,
                           long now,
                           HTableDescriptor htd) throws IOException {

    for (KeyValue edit : edits.getKeyValues()) {
      Log.Entry entry = Log
          .Entry
          .newBuilder()
          .setRegionInfo(info.getRegionNameAsString())
          .setKey(ByteString.copyFrom(edit.getRow()))
          .setFamily(ByteString.copyFrom(edit.getFamily()))
          .setColumn(ByteString.copyFrom(edit.getQualifier()))
          .setTs(edit.getTimestamp())
          .setValue(ByteString.copyFrom(edit.getValue()))
          .build();
      addEdit(entry);
    }
    return 0;
  }

  @Override
  public long append(HRegionInfo info, byte[] tableName, WALEdit edits, UUID clusterId, long now, HTableDescriptor htd) throws IOException {

    appendNoSync(info, tableName, edits, null, now, htd);
    this.sync();
    return now++;
  }

  public long getFilenum() {
    return this.fileNum;
  }
}
