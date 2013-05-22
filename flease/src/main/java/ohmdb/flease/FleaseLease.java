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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.jetlang.channels.AsyncRequest;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.channels.Request;
import org.jetlang.channels.RequestChannel;
import org.jetlang.core.Callback;
import org.jetlang.core.Disposable;
import org.jetlang.fibers.Fiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static ohmdb.flease.Flease.FleaseRequestMessage;
import static ohmdb.flease.Flease.Lease;

/**
 * A single lease object. Runs on a jetlang Fiber, and sends and responds to RPC requests.
 *
 */
public class FleaseLease {
    private static final Logger LOG = LoggerFactory.getLogger(FleaseLease.class);

    private final RequestChannel<FleaseRpcRequest,FleaseRpcReply> sendRpcChannel;
    private final RequestChannel<FleaseRpcRequest,FleaseRpcReply> incomingChannel =
            new MemoryRequestChannel<>();

    // TODO figure out the right types here.
    private final RequestChannel localOps = new MemoryRequestChannel();

    private final String leaseId;
    private final Fiber fiber;

    private final List<InetSocketAddress> peers;

    private long messageNumber = 0; // also named "r" in the algorithm.
    private int majority;
    // As per algorithm 1.
    private BallotNumber read = null;
    private BallotNumber write = null;
    // The actual lease data, aka "v" in Algorithm 1.
    private Lease lease = null;


    public FleaseLease(Fiber fiber, String leaseId, List<InetSocketAddress> peers,
                       RequestChannel<FleaseRpcRequest,FleaseRpcReply> sendRpcChannel) {
        this.fiber = fiber;
        this.leaseId = leaseId;
        this.peers = peers;
        this.majority = calculateMajority(peers.size());
        this.sendRpcChannel = sendRpcChannel;

        incomingChannel.subscribe(fiber, new Callback<Request<FleaseRpcRequest, FleaseRpcReply>>() {
            @Override
            public void onMessage(Request<FleaseRpcRequest, FleaseRpcReply> message) {
                 onIncomingMessage(message);
            }
        });
    }

    public ListenableFuture<Lease> read() {
        final SettableFuture<Lease> future = SettableFuture.create();

        fiber.execute(new Runnable() {
            @Override
            public void run() {
                // READ logic goes here?
                doRead(future);
            }
        });

        return future;
    }


    private void doRead(final SettableFuture<Lease> future) {
        try {
            // choose a new ballot number:
            // TODO use a "real" process id, not just 0.
            BallotNumber k = new BallotNumber(System.currentTimeMillis(), messageNumber++, 0);
            // outgoing message:
            FleaseRequestMessage outMsg = FleaseRequestMessage.newBuilder()
                    .setMessageType(FleaseRequestMessage.MessageType.READ)
                    .setLeaseId(leaseId)
                    .setK(k.getMessage()).build();

            final List<FleaseRpcReply> replies = new ArrayList<>(peers.size());

            final Disposable timeout = fiber.schedule(new Runnable() {
                @Override
                public void run() {
                    future.setException(new FleaseReadTimeoutException(replies, majority));
                }
            }, 20, TimeUnit.SECONDS);

            // Send to all processes:
            for (InetSocketAddress peer : peers) {
                FleaseRpcRequest rpcRequest = new FleaseRpcRequest(outMsg, peer);

                AsyncRequest.withOneReply(fiber, sendRpcChannel, rpcRequest, new Callback<FleaseRpcReply>() {
                    @Override
                    public void onMessage(FleaseRpcReply message) {
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

    private void checkReadReplies(List<FleaseRpcReply> replies, SettableFuture<Lease> future) {
        try {
            // every reply should be a ackREAD.
            BallotNumber largest = null;
            Lease v = null;
            for(FleaseRpcReply reply : replies) {
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
    public RequestChannel<FleaseRpcRequest, FleaseRpcReply> getIncomingChannel() {
        return incomingChannel;
    }

    /********* Incoming message callbacks belowith here *************/
    // Reactive/incoming message callbacks here.
    private void onIncomingMessage(Request<FleaseRpcRequest, FleaseRpcReply> message) {
        FleaseRequestMessage.MessageType messageType = message.getRequest().message.getMessageType();
        switch (messageType) {
            case READ:
                doMessageRead(message);
                break;

            case WRITE:
                doMessageWrite(message);
                break;
        }
    }

    private void doMessageWrite(Request<FleaseRpcRequest, FleaseRpcReply> message) {
        BallotNumber k = message.getRequest().getBallotNumber();
        // If write > k or read > k THEN
        if (k.compareTo(write) <= 0
                || k.compareTo(read) <= 0) {
            // send nackWRITE, k
            message.reply(FleaseRpcReply.getNackWriteMessage(message.getRequest(), k));
        } else {
            write = k;
            lease = message.getRequest().getLeaseData();
            // send ackWRITE, k
            message.reply(FleaseRpcReply.getAckWriteMessage(message.getRequest(), k));
        }
    }

    private void doMessageRead(Request<FleaseRpcRequest, FleaseRpcReply> message) {
        BallotNumber k = message.getRequest().getBallotNumber();
        // If write >= k or read >= k
        if (k.compareTo(write) < 0
                || k.compareTo(read) < 0) {
            // send (nackREAD, k) to p(j)
            message.reply(FleaseRpcReply.getNackReadMessage(message.getRequest(), k));
        } else {
            read = k;
            // send (ackREAD,k,write(i),v(i)) to p(j)
            message.reply(FleaseRpcReply.getAckReadMessage(message.getRequest(),
                    k, write, lease));
        }
    }

}
