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

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import static ohmdb.flease.Flease.FleaseReplyMessage;

/**
 * jetlang wrapper message
 */
public class FleaseRpcReply extends RetriedMessage {

    public final FleaseReplyMessage message;
    public final InetSocketAddress remoteAddress;

    public FleaseRpcReply(FleaseReplyMessage message, InetSocketAddress remoteAddress) {
        // TODO change this default
        super(10, 2, TimeUnit.SECONDS);
        // include information about the sender?
        this.message = message;
        this.remoteAddress = remoteAddress;
    }

    public boolean isNackRead() {
        return message.getMessageType() == FleaseReplyMessage.MessageType.nackREAD;
    }

    public boolean isAckRead() {
        return message.getMessageType() == FleaseReplyMessage.MessageType.ackREAD;
    }

    public BallotNumber getK() {
        return new BallotNumber(message.getK());
    }

    public BallotNumber getKPrime() {
        return new BallotNumber(message.getKprime());
    }

    public Flease.Lease getLease() {
        return message.getLease();
    }

    // Static constructors for convenience.
    public static FleaseRpcReply getNackReadMessage(FleaseRpcRequest inReplyTo, BallotNumber k) {
        // Copy the message id, lease id from the request.
        FleaseReplyMessage.Builder builder = FleaseReplyMessage.newBuilder();
        builder.setMessageType(FleaseReplyMessage.MessageType.nackREAD)
                .setMessageId(inReplyTo.message.getMessageId())
                .setLeaseId(inReplyTo.message.getLeaseId())
                .setK(k.getMessage());
        return new FleaseRpcReply(builder.build(), inReplyTo.remoteAddress);
    }

    public static FleaseRpcReply getAckReadMessage(FleaseRpcRequest inReplyTo,
                                                   BallotNumber k,
                                                   BallotNumber write,
                                                   Flease.Lease value) {
        FleaseReplyMessage.Builder builder = FleaseReplyMessage.newBuilder();
        builder.setMessageType(FleaseReplyMessage.MessageType.ackREAD)
                .setMessageId(inReplyTo.message.getMessageId())
                .setLeaseId(inReplyTo.message.getLeaseId())
                .setK(k.getMessage())
                .setKprime(write.getMessage())
                .setLease(value);
        return new FleaseRpcReply(builder.build(), inReplyTo.remoteAddress);
    }

    public static FleaseRpcReply getNackWriteMessage(FleaseRpcRequest inReplyTo, BallotNumber k) {
        // Copy the message id, lease id from the request.
        FleaseReplyMessage.Builder builder = FleaseReplyMessage.newBuilder();
        builder.setMessageType(FleaseReplyMessage.MessageType.nackWRITE)
                .setMessageId(inReplyTo.message.getMessageId())
                .setLeaseId(inReplyTo.message.getLeaseId())
                .setK(k.getMessage());
        return new FleaseRpcReply(builder.build(), inReplyTo.remoteAddress);
    }

    public static FleaseRpcReply getAckWriteMessage(FleaseRpcRequest inReplyTo, BallotNumber k) {
        // Copy the message id, lease id from the request.
        FleaseReplyMessage.Builder builder = FleaseReplyMessage.newBuilder();
        builder.setMessageType(FleaseReplyMessage.MessageType.ackWRITE)
                .setMessageId(inReplyTo.message.getMessageId())
                .setLeaseId(inReplyTo.message.getLeaseId())
                .setK(k.getMessage());
        return new FleaseRpcReply(builder.build(), inReplyTo.remoteAddress);
    }
}
