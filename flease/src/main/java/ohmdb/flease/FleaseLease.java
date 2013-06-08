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
import static ohmdb.flease.Flease.Lease;

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

    private long messageNumber = 0; // also named "r" in the algorithm.
    private int majority;
    // As per algorithm 1.
    private BallotNumber read = null;
    private BallotNumber write = null;
    // The actual lease data, aka "v" in Algorithm 1.
    private Lease lease = null;


    /**
     * A single instance of a flease lease.
     * @param fiber the fiber to run this lease on
     * @param leaseId the leaseId that uniquely identifies this lease. This could be a virtual resource name.
     * @param myId the UUID of this node.
     * @param peers the UUIDs of my peers, it should contain myId as well (I'm my own peer)
     * @param sendRpcChannel the rpc channel I'll use to send requests on
     */
    public FleaseLease(Fiber fiber, String leaseId, UUID myId,
                       List<UUID> peers,
                       RequestChannel<OutgoingRpcRequest,IncomingRpcReply> sendRpcChannel) {
        this.fiber = fiber;
        this.leaseId = leaseId;
        this.myId = myId;
        this.peers = ImmutableList.copyOf(peers);

        assert this.peers.contains(this.myId);

        this.majority = calculateMajority(peers.size());
        this.sendRpcChannel = sendRpcChannel;

        incomingChannel.subscribe(fiber, new Callback<Request<IncomingRpcRequest,OutgoingRpcReply>>() {
            @Override
            public void onMessage(Request<IncomingRpcRequest,OutgoingRpcReply> message) {
                 onIncomingMessage(message);
            }
        });
        // set the 'lease' to the default/empty, which will serve as 'null' for now:
        lease = Lease.getDefaultInstance();
    }


    public ListenableFuture<Lease> write(final String datum) {
        final SettableFuture<Lease> future = SettableFuture.create();

        fiber.execute(new Runnable() {
            @Override
            public void run() {
                writeInternal(future, datum);
            }
            });

        return future;
    }

    private void writeInternal(final SettableFuture<Lease> future, String datum) {
        try {
            // Choose a lease expiry:
            long exp = System.currentTimeMillis() + 10; // TODO param this 10 seconds
            final Flease.Lease newLease = Flease.Lease.newBuilder().setDatum(datum)
                    .setLeaseExpiry(exp).build();

            BallotNumber k = getNextBallotNumber();

            FleaseRequestMessage outMsg = FleaseRequestMessage.newBuilder()
                    .setMessageType(FleaseRequestMessage.MessageType.WRITE)
                    .setLeaseId(leaseId)
                    .setK(k.getMessage())
                    .setLease(newLease).build();

            final List<IncomingRpcReply> replies = new ArrayList<>(peers.size());

            final Disposable timeout = fiber.schedule(new Runnable() {
                @Override
                public void run() {
                    future.setException(new FleaseReadTimeoutException(replies, majority));
                }
            }, 20, TimeUnit.SECONDS);

            for (UUID peer : peers) {
                OutgoingRpcRequest rpcRequest = new OutgoingRpcRequest(outMsg, peer);

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
    public ListenableFuture<Lease> read() {
        final SettableFuture<Lease> future = SettableFuture.create();

        fiber.execute(new Runnable() {
            @Override
            public void run() {
                readInternal(future);
            }
        });

        return future;
    }


    private void readInternal(final SettableFuture<Lease> future) {
        try {
            // choose a new ballot number:
            // TODO use a "real" process id, not just 0.
            BallotNumber k = getNextBallotNumber();
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
                OutgoingRpcRequest rpcRequest = new OutgoingRpcRequest(outMsg, peer);

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
        return new BallotNumber(System.currentTimeMillis(), messageNumber++, 0);
    }

    private void checkReadReplies(List<IncomingRpcReply> replies, SettableFuture<Lease> future) {
        try {
            // every reply should be a ackREAD.
            BallotNumber largest = null;
            Lease v = null;
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
        if (k.compareTo(write) <= 0
                || k.compareTo(read) <= 0) {
            // send nackWRITE, k
            message.reply(OutgoingRpcReply.getNackWriteMessage(message.getRequest(), k));
        } else {
            write = k;
            lease = message.getRequest().getLeaseData();
            // send ackWRITE, k
            message.reply(OutgoingRpcReply.getAckWriteMessage(message.getRequest(), k));
        }
    }

    private void receiveRead(Request<IncomingRpcRequest, OutgoingRpcReply> message) {
        BallotNumber k = message.getRequest().getBallotNumber();
        // If write >= k or read >= k
        if (k.compareTo(write) < 0
                || k.compareTo(read) < 0) {
            // send (nackREAD, k) to p(j)
            message.reply(OutgoingRpcReply.getNackReadMessage(message.getRequest(), k));
        } else {
            read = k;
            // send (ackREAD,k,write(i),v(i)) to p(j)
            message.reply(OutgoingRpcReply.getAckReadMessage(message.getRequest(),
                    k, write, lease));
        }
    }

}
