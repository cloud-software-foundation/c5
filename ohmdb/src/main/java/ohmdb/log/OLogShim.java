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

import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import ohmdb.generated.Log;
import ohmdb.replication.InRamLog;
import ohmdb.replication.RaftInfoPersistence;
import ohmdb.replication.RaftInformationInterface;
import ohmdb.replication.ReplicatorInstance;
import ohmdb.replication.ReplicatorInstanceStateChange;
import ohmdb.replication.rpc.RpcRequest;
import ohmdb.replication.rpc.RpcWireReply;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.channels.RequestChannel;
import org.jetlang.fibers.PoolFiberFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A distributed WriteAheadLog using RAFT
 */
public class OLogShim extends OLog implements Syncable, HLog {
    private static final Logger LOG = LoggerFactory.getLogger(OLogShim.class);
    final Map<Long, ReplicatorInstance> replicators = new HashMap<>();
    final RequestChannel<RpcRequest, RpcWireReply> rpcChannel = new MemoryRequestChannel<>();
    final Map<Long, Mooring> moorings = new HashMap<>();
    final Map<String, Long> replicatorLookup = new HashMap<>();
    final List<Long> peerIds = new ArrayList<>();
    private final AtomicLong logSeqNum = new AtomicLong(0);
    private final UUID uuid;
    private final PoolFiberFactory fiberPool;


    public OLogShim(String basePath) throws IOException {
        super(basePath);
        this.uuid = UUID.randomUUID();
        this.fiberPool = new PoolFiberFactory(Executors.newCachedThreadPool());

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
        try {
            roll();
        } catch (ExecutionException | InterruptedException e) {
            throw new IOException(e);
        }
        return null;
    }

    @Override
    public byte[][] rollWriter(boolean force)
            throws IOException {
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
    public void sync() throws IOException {
        SettableFuture<Boolean> f = SettableFuture.create();
        this.sync(f);
    }

    @Override
    public long getSequenceNumber() {
        return logSeqNum.get();
    }

    @Override
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
                       byte[] tableName,
                       WALEdit edits,
                       long now,
                       HTableDescriptor htd) throws IOException {
        this.append(info, tableName, edits, uuid, now, htd);
    }

    //  @Override
    public void append(HRegionInfo info,
                       byte[] tableName,
                       WALEdit edits,
                       long now,
                       HTableDescriptor htd,
                       boolean isInMemstore) throws IOException {
        this.append(info, tableName, edits, uuid, now, htd);
    }

    @Override
    public long appendNoSync(HRegionInfo info,
                             byte[] tableName,
                             WALEdit edits,
                             UUID clusterId,
                             long now,
                             HTableDescriptor htd) throws IOException {
        ReplicatorInstance replicator = getReplicator(info);

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
                replicator.logData(entry.toByteArray());
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
        return 0;
    }

    private Channel<ReplicatorInstanceStateChange> stateChangeChannel = new MemoryChannel<>();

    private ReplicatorInstance getReplicator(HRegionInfo info) {
        if (replicatorLookup.containsKey(info.getRegionNameAsString())) {
            return replicators.get(replicatorLookup.get(info.getRegionNameAsString()));
        }
        long plusMillis = 0;

        // Some concurrent magic TODO
        long peerId = replicatorLookup.size();
        replicatorLookup.put(info.getRegionNameAsString(), peerId);
        peerIds.add(peerId);


        ReplicatorInstance replicator = new ReplicatorInstance(fiberPool.create(),
                peerId,
                "foobar",
                peerIds,
                new InRamLog(),
                new Info(plusMillis),
                new Persister(),
                rpcChannel,
                stateChangeChannel);
        replicators.put(peerId, replicator);
        return replicator;
    }

    private Mooring getMooring(long peerId) {
        if (this.moorings.containsKey(peerId)) {
            return this.moorings.get(peerId);
        }
        Mooring mooring = new Mooring(this, peerId);
        this.moorings.put(peerId, mooring);
        return mooring;
    }

    public long append(HRegionInfo info,
                       byte[] tableName,
                       WALEdit edits,
                       UUID clusterId,
                       long now,
                       HTableDescriptor htd) throws IOException {
        appendNoSync(info, tableName, edits, null, now, htd);
        this.sync();
        return ++now;
    }

    public long getFilenum() {
        return this.fileNum;
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