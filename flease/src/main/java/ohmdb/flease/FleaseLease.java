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
package ohmdb.flease;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import ohmdb.flease.rpc.IncomingRpcReply;
import ohmdb.flease.rpc.IncomingRpcRequest;
import ohmdb.flease.rpc.OutgoingRpcReply;
import ohmdb.flease.rpc.OutgoingRpcRequest;
import org.jetlang.channels.AsyncRequest;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.channels.Request;
import org.jetlang.channels.RequestChannel;
import org.jetlang.core.Callback;
import org.jetlang.core.Disposable;
import org.jetlang.fibers.Fiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static ohmdb.flease.Flease.FleaseRequestMessage;

/**
 * A single lease object. Runs on a jetlang Fiber, and sends and responds to RPC requests.
 *
 */
public class FleaseLease {
    private static final Logger LOG = LoggerFactory.getLogger(FleaseLease.class);

    private final RequestChannel<OutgoingRpcRequest,IncomingRpcReply> sendRpcChannel;
    private final RequestChannel<IncomingRpcRequest,OutgoingRpcReply> incomingChannel =
            new MemoryRequestChannel<>();

    // TODO figure out the right types here.
    private final RequestChannel localOps = new MemoryRequestChannel();

    private final String leaseId;
    private final Fiber fiber;

    final long myId;
    private final ImmutableList<Long> peers;
    private final String leaseValue;

    private long messageNumber = 0; // also named "r" in the algorithm.
    private int majority;
    // As per algorithm 1.
    private BallotNumber read = new BallotNumber();
    private BallotNumber write = new BallotNumber();
    // The actual lease data, aka "v" in Algorithm 1.
    private LeaseValue lease = new LeaseValue();

    // package local for testing
    final InformationInterface info;


    /**
     * A single instance of a flease lease.
     * @param fiber the fiber to run this lease on, not started
     * @param leaseValue The value string to use when creating leases.  This should be the IP/port/hostname
     *                   for the lease.
     * @param leaseId the leaseId that uniquely identifies this lease. This could be a virtual resource name.
     * @param myId the UUID of this node.
     * @param peers the UUIDs of my peers, it should contain myId as well (I'm my own peer)
     * @param sendRpcChannel the rpc channel I'll use to send requests on
     */
    public FleaseLease(Fiber fiber, InformationInterface info,
                       String leaseValue,
                       String leaseId,
                       long myId,

                       List<Long> peers,
                       RequestChannel<OutgoingRpcRequest,IncomingRpcReply> sendRpcChannel) {
        this.fiber = fiber;
        this.leaseId = leaseId;
        this.leaseValue = leaseValue;
        this.myId = myId;
        this.peers = ImmutableList.copyOf(peers);
        this.info = info;

        assert this.peers.contains(this.myId);

        this.majority = calculateMajority(peers.size());
        this.sendRpcChannel = sendRpcChannel;

        incomingChannel.subscribe(fiber, new Callback<Request<IncomingRpcRequest,OutgoingRpcReply>>() {
            @Override
            public void onMessage(Request<IncomingRpcRequest,OutgoingRpcReply> message) {
                 onIncomingMessage(message);
            }
        });

        fiber.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                  maintainLease();
            }
        }, 200, 200, TimeUnit.MILLISECONDS);

        this.fiber.start();
    }

    /**
     * Maintain the lease for the system.
     * Things we may do:
     * * Renew the lease if we own it
     * * Renew the lease even if we dont own it
     * * Other stuff
     */
    private void maintainLease() {
        // always call getLease.  If we arent the lease owner, we'll propagate it, otherwise we'll be attempting
        // to renew and maintain our own lease.
        ListenableFuture<LeaseValue> future = getLease();
        Futures.addCallback(future, new FutureCallback<LeaseValue>() {
            @Override
            public void onSuccess(LeaseValue result) {
                LOG.debug("{} maintainLease successful getLease: {}", getId(), result);
            }

            @Override
            public void onFailure(Throwable t) {
                LOG.debug("{} maintainLease exception {}", getId(), t.toString());
            }
        }, fiber);
    }

    /**
     * Returns the current lease - may be null!
     * @return the lease value or null if no lease is set.
     */
    public LeaseValue getLeaseValue() {
        return lease;
    }

    /**
     * Returns the ballot number associated with the lease.
     * @return ballot number, or the "0" ballot number if no lease.
     */
    public BallotNumber getWriteBallot() {
        return write;
    }

    /**
     * Get the string that identifies this process (uuid + hashCode for ballots)
     * @return descriptive string for debugging
     */
    public long getId() {
        return myId;
    }

    public boolean isLeaseOwner() {
        if (lease == null) return false;
        return myId == lease.leaseOwner;
    }

    public ListenableFuture<LeaseValue> getLease() {
        SettableFuture<LeaseValue> future = SettableFuture.create();
        getLease(future);
        return future;
    }

    protected void getLease(final SettableFuture<LeaseValue> future) {
        final BallotNumber k = getNextBallotNumber();

        // We are NOT (or may not be) on the fiber at this point, so call the non-fiber version.
        ListenableFuture<LeaseValue> read = read(k);
        // The future becomes a way to chain one activity on the fiber to the next:
        Futures.addCallback(read, new FutureCallback<LeaseValue>() {
            @Override
            public void onSuccess(LeaseValue readValue) {

                if (readValue.isBefore(info.currentTimeMillis()) && readValue.isAfter(info.currentTimeMillis(), info)) {
                    // wait for an amount of time then retry getLease:

                    long delay = (readValue.leaseExpiry + info.getEpsilon()) - info.currentTimeMillis();
                    LOG.debug("{} I think the lease is invalid: {}, but I need to delay: {}", getId(), readValue, delay);
                    fiber.schedule(new Runnable() {
                        @Override
                        public void run() {
                            getLease(future);
                        }
                    }, delay, TimeUnit.MILLISECONDS);
                    // no more processing at this time.
                    return;
                }

                // Lease renewal.
                LeaseValue lease = null;

                if (readValue.isBefore(info.currentTimeMillis())) {
                    LOG.info("{} I think this lease is invalid: {} based on time: {}", getId(), readValue, info.currentTimeMillis());
                    lease = new LeaseValue(leaseValue, info.currentTimeMillis() + info.getLeaseLength(), myId);
                } else if (myId == readValue.leaseOwner) {
                    LOG.info("{} This is my lease, i will renew it into the future!", getId());
                    lease = new LeaseValue(leaseValue, info.currentTimeMillis() + info.getLeaseLength(), myId);
                } else {
                    LOG.debug("{} pushing existing lease out to the group {}", getId(), readValue);
                    lease = readValue;
                }

                // running on the fiber. can directly call internalWrite()
                writeInternal(future, k, lease);
            }

            @Override
            public void onFailure(Throwable t) {
                future.setException(t);
            }
        }, fiber);
    }

    public void write(final LeaseValue newLease, final SettableFuture<LeaseValue> future) {
        fiber.execute(new Runnable() {
            @Override
            public void run() {
                writeInternal(future, getNextBallotNumber(), newLease);
            }
        });
    }

    public ListenableFuture<LeaseValue> write(final LeaseValue newLease) {
        final SettableFuture<LeaseValue> future = SettableFuture.create();

        fiber.execute(new Runnable() {
            @Override
            public void run() {
                writeInternal(future, getNextBallotNumber(), newLease);
            }
            });

        return future;
    }

    /**
     * Only call when executed ON the fiber runnable.  If you are already on the fiber runnable, you may
     * call this, otherwise consider using write().
     * @param future callback ftw
     * @param k the "k"/ballot number value.
     * @param newLease the lease object
     */
    private void writeInternal(final SettableFuture<LeaseValue> future,
                               final BallotNumber k,
                               final LeaseValue newLease) {
        try {
            FleaseRequestMessage outMsg = FleaseRequestMessage.newBuilder()
                    .setMessageType(FleaseRequestMessage.MessageType.WRITE)
                    .setLeaseId(leaseId)
                    .setK(k.getMessage())
                    .setLease(newLease.getMessage()).build();

            final List<IncomingRpcReply> replies = new ArrayList<>(peers.size());

            final Disposable timeout = fiber.schedule(new Runnable() {
                @Override
                public void run() {
                    future.setException(new FleaseWriteTimeoutException(replies, majority));
                }
            }, 20, TimeUnit.SECONDS);

            for (Long peer : peers) {
                OutgoingRpcRequest rpcRequest = new OutgoingRpcRequest(myId, outMsg, peer);

                AsyncRequest.withOneReply(fiber, sendRpcChannel, rpcRequest, new Callback<IncomingRpcReply>() {
                    @Override
                    public void onMessage(IncomingRpcReply message) {
                        // in theory it's possible for this exception call to fail.
                        if (message.isNackWrite()) {
                            if (!future.setException(new NackWriteException(message))) {
                                LOG.warn("{} write unable to set future exception nackWRITE {}", getId(), message);
                            }
                            // kill the timeout, let's GTFO
                            timeout.dispose();
                            return; // well not much to do anymore.
                        }

                        replies.add(message);

                        // TODO add delay processing so we confirm AFTER we have given all processes a chance to report in
                        if (replies.size() >= majority) {
                            timeout.dispose();

                            // not having received any nackWRITE, we can declare success:
                            if (!future.set(newLease)) {
                                LOG.warn("{} write unable to set future for new lease {}",
                                        getId(), newLease);
                            }
                        }
                    }
                });
            }
        } catch (Throwable t) {
            future.setException(t);
        }
    }

    /**
     * Procedure READ(k) from Algorithm 1.
     * @return
     */
    public ListenableFuture<LeaseValue> read(final BallotNumber k) {
        final SettableFuture<LeaseValue> future = SettableFuture.create();

        fiber.execute(new Runnable() {
            @Override
            public void run() {
                readInternal(future, k);
            }
        });

        return future;
    }


    private void readInternal(final SettableFuture<LeaseValue> future, final BallotNumber k) {
        try {
            // outgoing message:
            FleaseRequestMessage outMsg = FleaseRequestMessage.newBuilder()
                    .setMessageType(FleaseRequestMessage.MessageType.READ)
                    .setLeaseId(leaseId)
                    .setK(k.getMessage()).build();

            final List<IncomingRpcReply> replies = new ArrayList<>(peers.size());

            final Disposable timeout = fiber.schedule(new Runnable() {
                @Override
                public void run() {
                    future.setException(new FleaseReadTimeoutException(replies, majority));
                }
            }, 20, TimeUnit.SECONDS);

            // Send to all processes:
            for (long peer : peers) {
                OutgoingRpcRequest rpcRequest = new OutgoingRpcRequest(myId, outMsg, peer);

                AsyncRequest.withOneReply(fiber, sendRpcChannel, rpcRequest, new Callback<IncomingRpcReply>() {
                    @Override
                    public void onMessage(IncomingRpcReply message) {
                        if (message.isNackRead()) {
                            // even a single is a failure.
                            if (!future.setException(new NackReadException(message))) {
                                LOG.warn("{} read reply processing, got NackREAD but unable to set failure {}",
                                        getId(), message);
                            }
                            timeout.dispose(); // ditch the timeout.
                            return;
                        }

                        replies.add(message);

                        if (replies.size() >= majority) {
                            timeout.dispose(); // cancel the timeout.
                            checkReadReplies(replies, future);

                            // TODO Set a timeout to actually do the work so we have a chance for more messages.
//                            fiber.schedule(new Runnable() {
//                                @Override
//                                public void run() {
//                                    checkReadReplies(replies, future);
//                                }
//                            }, 20, TimeUnit.MILLISECONDS);
                        }
                    }
                });
            }

        } catch (Throwable t) {
            future.setException(t);
        }
    }

    private BallotNumber getNextBallotNumber() {
        return new BallotNumber(info.currentTimeMillis(), messageNumber++, myId);
    }

    private void checkReadReplies(List<IncomingRpcReply> replies, SettableFuture<LeaseValue> future) {
        try {
            // every reply should be a ackREAD.
            BallotNumber largest = null;
            LeaseValue v = null;
            for(IncomingRpcReply reply : replies) {
                // if kPrime > largest
                BallotNumber kPrime = reply.getKPrime();
                if (kPrime.compareTo(largest) > 0) {
                    largest = kPrime;
                    v = reply.getLease();
                }
            }
            if (!future.set(v)) {
                LOG.warn("{} checkReadReplies, unable to set future for {}", getId(), v);
            }
        } catch (Throwable e) {
            if (!future.setException(e)) {
                LOG.warn("{} checkReadReplies, unable to set exception future, {}", (Object)e);
            }
        }
    }


    private int calculateMajority(int peerCount) {
        return (int)Math.ceil((peerCount+1)/2.0);
    }


    /**
     * Get the lease id that this lease object represents!
     * @return
     */
    public String getLeaseId() {
        return leaseId;
    }

    /**
     * Get the request channel for incoming messages for this lease object.
     * The RPC system is expected to dispatch incoming messages to this
     * @return
     */
    public RequestChannel<IncomingRpcRequest,OutgoingRpcReply> getIncomingChannel() {
        return incomingChannel;
    }

    /********* Incoming message callbacks belowith here *************/
    // Reactive/incoming message callbacks here.
    private void onIncomingMessage(Request<IncomingRpcRequest, OutgoingRpcReply> message) {
        FleaseRequestMessage.MessageType messageType = message.getRequest().message.getMessageType();
        switch (messageType) {
            case READ:
                receiveRead(message);
                break;

            case WRITE:
                receiveWrite(message);
                break;
        }
    }

    private void receiveWrite(Request<IncomingRpcRequest, OutgoingRpcReply> message) {
        BallotNumber k = message.getRequest().getBallotNumber();
        // If write > k or read > k THEN
        if (write.compareTo(k, info) > 0
                || read.compareTo(k, info) > 0) {
            // send nackWRITE, k
            LOG.debug("{} sending nackWRITE, rejected ballot {} because I already have (r;w) {} ; {}", getId(), k, read, write);
            message.reply(OutgoingRpcReply.getNackWriteMessage(message.getRequest(), k));
        } else {
            write = k;
            lease = message.getRequest().getLease();
            LOG.info("{} new lease value written [{}] with k {}", getId(), lease, write);

            // send ackWRITE, k
            message.reply(OutgoingRpcReply.getAckWriteMessage(message.getRequest(), k));
        }
    }

    private void receiveRead(Request<IncomingRpcRequest, OutgoingRpcReply> message) {
        BallotNumber k = message.getRequest().getBallotNumber();
        // If write >= k or read >= k
        if (write.compareTo(k, info) >= 0
                || read.compareTo(k, info) >= 0) {
            // send (nackREAD, k) to p(j)
            LOG.debug("{} Sending nackREAD, rejecting ballot {} because I already have (r;w) {} ; {}", getId(), k, read, write);
            message.reply(OutgoingRpcReply.getNackReadMessage(message.getRequest(), k));
        } else {
            LOG.debug("{} onRead, setting 'read' to : {} (was: {})", getId(), k, read);
            read = k;
            // send (ackREAD,k,write(i),v(i)) to p(j)
            message.reply(OutgoingRpcReply.getAckReadMessage(message.getRequest(),
                    k, write, lease));
        }
    }

    public void dispose() {
        fiber.dispose();
    }
}
