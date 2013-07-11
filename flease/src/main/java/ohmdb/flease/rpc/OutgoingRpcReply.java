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
package ohmdb.flease.rpc;

import ohmdb.flease.BallotNumber;
import ohmdb.flease.Flease;
import ohmdb.flease.LeaseValue;

import static ohmdb.flease.Flease.FleaseReplyMessage;

/**
 * A reply to an IncomingRpcRequest.
 */
public class OutgoingRpcReply {

    public final long messageIdInReplyTo;
    public final FleaseReplyMessage message;
    public final long to;

    public OutgoingRpcReply(IncomingRpcRequest requestInReplyTo, FleaseReplyMessage message) {
        this.messageIdInReplyTo = requestInReplyTo.messageId;
        this.message = message;
        this.to = requestInReplyTo.from;
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
    public static OutgoingRpcReply getNackReadMessage(IncomingRpcRequest inReplyTo, BallotNumber k) {
        FleaseReplyMessage.Builder builder = FleaseReplyMessage.newBuilder();
        builder.setMessageType(FleaseReplyMessage.MessageType.nackREAD)
                .setLeaseId(inReplyTo.message.getLeaseId())
                .setK(k.getMessage());
        return new OutgoingRpcReply(inReplyTo, builder.build());
    }

    public static OutgoingRpcReply getAckReadMessage(IncomingRpcRequest inReplyTo,
                                                   BallotNumber k,
                                                   BallotNumber write,
                                                   LeaseValue value) {
        FleaseReplyMessage.Builder builder = FleaseReplyMessage.newBuilder();
        builder.setMessageType(FleaseReplyMessage.MessageType.ackREAD)
                .setLeaseId(inReplyTo.message.getLeaseId())
                .setK(k.getMessage())
                .setKprime(write.getMessage())
                .setLease(value.getMessage());
        return new OutgoingRpcReply(inReplyTo,  builder.build());
    }

    public static OutgoingRpcReply getNackWriteMessage(IncomingRpcRequest inReplyTo, BallotNumber k) {
        FleaseReplyMessage.Builder builder = FleaseReplyMessage.newBuilder();
        builder.setMessageType(FleaseReplyMessage.MessageType.nackWRITE)
                .setLeaseId(inReplyTo.message.getLeaseId())
                .setK(k.getMessage());
        return new OutgoingRpcReply(inReplyTo, builder.build());
    }

    public static OutgoingRpcReply getAckWriteMessage(IncomingRpcRequest inReplyTo, BallotNumber k) {
        FleaseReplyMessage.Builder builder = FleaseReplyMessage.newBuilder();
        builder.setMessageType(FleaseReplyMessage.MessageType.ackWRITE)
                .setLeaseId(inReplyTo.message.getLeaseId())
                .setK(k.getMessage());
        return new OutgoingRpcReply(inReplyTo, builder.build());
    }
}
