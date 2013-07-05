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
import java.util.UUID;
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

    private final UUID myId;
    private final ImmutableList<UUID> peers;
    private final String leaseValue;

    private long messageNumber = 0; // also named "r" in the algorithm.
    private int majority;
    // As per algorithm 1.
    private BallotNumber read = new BallotNumber();
    private BallotNumber write = new BallotNumber();
    // The actual lease data, aka "v" in Algorithm 1.
    private LeaseValue lease = new LeaseValue();
    private final InformationInterface info;


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
                       String leaseId, UUID myId,

                       List<UUID> peers,
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

        fiber.start();
    }

    // return the configured lease value
    public String getLeaseValue() {
        return leaseValue;
    }
    public UUID getId() {
        return myId;
    }

    public ListenableFuture<LeaseValue> getLease() {
        final SettableFuture<LeaseValue> future = SettableFuture.create();
        final BallotNumber k = getNextBallotNumber();

        // We are NOT on the fiber at this point, so call the non-fiber version.
        ListenableFuture<LeaseValue> read = read(k);
        // The future becomes a way to chain one activity on the fiber to the next:
        Futures.addCallback(read, new FutureCallback<LeaseValue>() {
            @Override
            public void onSuccess(LeaseValue readValue) {
                LeaseValue lease = null;

                // TODO implement the full Flease algorithm including waiting.
                if (readValue.isBefore(info.currentTimeMillis())) {
                    lease = new LeaseValue(leaseValue, info.currentTimeMillis() + info.getLeaseLength());
                } else {
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


        return future;
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
                    future.setException(new FleaseReadTimeoutException(replies, majority));
                }
            }, 20, TimeUnit.SECONDS);

            for (UUID peer : peers) {
                OutgoingRpcRequest rpcRequest = new OutgoingRpcRequest(myId, outMsg, peer);

                AsyncRequest.withOneReply(fiber, sendRpcChannel, rpcRequest, new Callback<IncomingRpcReply>() {
                    @Override
                    public void onMessage(IncomingRpcReply message) {
                        if (message.isNackWrite())
                            future.setException(new NackWriteException(message));

                        replies.add(message);

                        if (replies.size() >= majority) {
                            timeout.dispose();

                            // not having received any nackWRITE, we can declare success:
                            future.set(newLease);
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
            for (UUID peer : peers) {
                OutgoingRpcRequest rpcRequest = new OutgoingRpcRequest(myId, outMsg, peer);

                AsyncRequest.withOneReply(fiber, sendRpcChannel, rpcRequest, new Callback<IncomingRpcReply>() {
                    @Override
                    public void onMessage(IncomingRpcReply message) {
                        if (message.isNackRead()) {
                            // even a single is a failure.
                            future.setException(new NackReadException(message));
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
        return new BallotNumber(info.currentTimeMillis(), messageNumber++, 0);
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
            future.set(v);
        } catch (Throwable e) {
            future.setException(e);
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
        if (k.compareTo(write) < 0
                || k.compareTo(read) < 0) {
            // send nackWRITE, k
            message.reply(OutgoingRpcReply.getNackWriteMessage(message.getRequest(), k));
        } else {
            write = k;
            lease = message.getRequest().getLease();
            LOG.info("{} new lease value written [{}] with k {}", myId, lease, write);

            // send ackWRITE, k
            message.reply(OutgoingRpcReply.getAckWriteMessage(message.getRequest(), k));
        }
    }

    private void receiveRead(Request<IncomingRpcRequest, OutgoingRpcReply> message) {
        BallotNumber k = message.getRequest().getBallotNumber();
        // If write >= k or read >= k
        if (k.compareTo(write) <= 0
                || k.compareTo(read) <= 0) {
            // send (nackREAD, k) to p(j)
            LOG.debug("Sending nackREAD, {}", k);
            message.reply(OutgoingRpcReply.getNackReadMessage(message.getRequest(), k));
        } else {
            LOG.debug("onRead, setting 'read' to : {} (was: {})", k, read);
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
