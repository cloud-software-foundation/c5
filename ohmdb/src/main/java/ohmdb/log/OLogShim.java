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
import ohmdb.interfaces.ReplicationModule;
import ohmdb.replication.RaftInfoPersistence;
import ohmdb.replication.RaftInformationInterface;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A distributed WriteAheadLog using RAFT
 */
public class OLogShim implements Syncable, HLog {
    private static final Logger LOG = LoggerFactory.getLogger(OLogShim.class);
    private final AtomicLong logSeqNum = new AtomicLong(0);
    private final UUID uuid;
    private final ReplicationModule.Replicator replicatorInstance;
    private final String tabletId;

    public OLogShim(ReplicationModule.Replicator replicatorInstance) {
        this.uuid = UUID.randomUUID();
        this.replicatorInstance = replicatorInstance;
        this.tabletId = replicatorInstance.getQuorumId();
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
        LOG.error("unsupported registerWALActionsListener, NOOP (probably causing bugs!)");
    }

    @Override
    public boolean unregisterWALActionsListener(WALActionsListener listener) {
        LOG.error("unsupported unregisterWALActionsListener, returning false (probably causing bugs!)");
        return false;
    }

    @Override
    public WALCoprocessorHost getCoprocessorHost() {
        return null;
    }

    @Override
    public byte[][] rollWriter() throws IOException {
        // TODO this is not passed thru to underlying OLog implementation.
//        try {
//            roll();
//        } catch (ExecutionException | InterruptedException e) {
//            throw new IOException(e);
//        }
        return null;
    }

    @Override
    public byte[][] rollWriter(boolean force)
            throws IOException {
        return rollWriter();
    }

    @Override
    public void close() throws IOException {
       // TODO take this as a clue to turn off the ReplicationInstance we depend on.

    }

    public void closeAndDelete() throws IOException {
        // TODO still need more info to make this reasonably successful.
        close();
        // TODO can't delete at this level really.  Maybe we need to mark
        // that this quorumId is no longer useful?
//        boolean success = this.logPath.toFile().delete();
//        if (!success) {
//            throw new IOException("Unable to close and delete: "
//                    + this.logPath.toFile().toString());
//        }

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
    public void sync() throws IOException {
        // TODO make this wait for the most recently written log-id to be made visible.
    }

    @Override
    public long getSequenceNumber() {
        return logSeqNum.get();
    }

    @Override
    // TODO this is a problematic call because RAFT is in charge of the log-ids. We must
    // TODO  depreciate this call, and bubble the consequences thruout the system.
    public void setSequenceNumber(long newValue) {
        for (long id = this.logSeqNum.get(); id < newValue &&
                !this.logSeqNum.compareAndSet(id, newValue); id = this.logSeqNum.get()) {
            LOG.debug("Changed sequenceid from " + id + " to " + newValue);
        }
    }

    @Override
    public long obtainSeqNum() {
        return this.logSeqNum.incrementAndGet();
    }


    @Override
    public void append(HRegionInfo info,
                       TableName tableName,
                       WALEdit edits,
                       long now,
                       HTableDescriptor htd) throws IOException {
        this.append(info, tableName, edits, uuid, now, htd);
    }

    @Override
    public void append(HRegionInfo info,
                       TableName tableName,
                       WALEdit edits,
                       long now,
                       HTableDescriptor htd,
                       boolean isInMemstore) throws IOException {
        this.append(info, tableName, edits, uuid, now, htd);
    }

    @Override
    public long appendNoSync(HRegionInfo info, TableName tableName, WALEdit edits, List<UUID> clusterIds, long now, HTableDescriptor htd) throws IOException {
        //ReplicatorInstance replicator = getReplicator(info);

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
            try {
                // our replicator knows what quorumId/tabletId we are.
                replicatorInstance.logData(entry.toByteArray());
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
        return 0;
    }

    private Channel<ReplicationModule.ReplicatorInstanceStateChange> stateChangeChannel = new MemoryChannel<>();
    private Channel<ReplicationModule.IndexCommitNotice> commitNoticeChannel = new MemoryChannel<>();

//    private ReplicatorInstance getReplicator(HRegionInfo info) {
//        if (replicatorLookup.containsKey(info.getRegionNameAsString())) {
//            return replicators.get(replicatorLookup.get(info.getRegionNameAsString()));
//        }
//        long plusMillis = 0;
//
//        // Some concurrent magic TODO
//        long peerId = replicatorLookup.size();
//        replicatorLookup.put(info.getRegionNameAsString(), peerId);
//        peerIds.add(peerId);
//
//
//        ReplicatorInstance replicator = new ReplicatorInstance(fiberPool.create(),
//                peerId,
//                "foobar",
//                peerIds,
//                new InRamLog(),
//                new Info(plusMillis),
//                new Persister(),
//                rpcChannel,
//                stateChangeChannel,
//                commitNoticeChannel);
//        replicators.put(peerId, replicator);
//        return replicator;
//    }

//    private Mooring getMooring(String quorumId) {
//        if (this.moorings.containsKey(quorumId)) {
//            return this.moorings.get(quorumId);
//        }
////        Mooring mooring = new Mooring(this, quorumId);
////        this.moorings.put(quorumId, mooring);
////        return mooring;
//        return null;
//    }

    public long append(HRegionInfo info,
                       TableName tableName,
                       WALEdit edits,
                       UUID clusterId,
                       long now,
                       HTableDescriptor htd) throws IOException {
        appendNoSync(info, tableName, edits, null, now, htd);
        this.sync();
        return ++now;
    }

    // TODO XXX passthru no longer valid, this call does the wrong thing now.
    public long getFilenum() {
        return 0;
    }

    public static class Info implements RaftInformationInterface {

        public final long offset;

        public Info(long offset) {
            this.offset = offset;
        }

        @Override
        public long currentTimeMillis() {
            return System.currentTimeMillis() + offset;
        }

        @Override
        public long electionCheckRate() {
            return 100;
        }

        @Override
        public long electionTimeout() {
            return 1000;
        }

        @Override
        public long groupCommitDelay() {
            return 50;
        }
    }

    public static class Persister implements RaftInfoPersistence {
        @Override
        public long readCurrentTerm(String quorumId) {
            return 0;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public long readVotedFor(String quorumId) {
            return 0;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void writeCurrentTermAndVotedFor(String quorumId, long currentTerm, long votedFor) {
            //To change body of implemented methods use File | Settings | File Templates.
        }
    }
}