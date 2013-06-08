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

import java.util.UUID;

/**
 * A reply message in response to a previously sent request message.  Constructed by RPC layer.
 */
public class IncomingRpcReply {
    public final Flease.FleaseReplyMessage message;
    public final UUID from;

    IncomingRpcReply(Flease.FleaseReplyMessage message, UUID from) {
        this.message = message;
        this.from = from;
    }

    public BallotNumber getK() {
        return new BallotNumber(message.getK());
    }

    public boolean isNackRead() {
        return Flease.FleaseReplyMessage.MessageType.nackREAD.equals(message.getMessageType());
    }

    public BallotNumber getKPrime() {
        return new BallotNumber(message.getKprime());
    }

    public Flease.Lease getLease() {
        return message.getLease();
    }

    public boolean isNackWrite() {
        return Flease.FleaseReplyMessage.MessageType.nackWRITE.equals(message.getMessageType());
    }
}
