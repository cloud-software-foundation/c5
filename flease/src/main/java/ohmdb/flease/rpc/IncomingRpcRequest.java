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
import ohmdb.flease.LeaseValue;

import java.util.UUID;

import static ohmdb.flease.Flease.FleaseRequestMessage;

/**
 * An incoming request from a peer.
 */
public class IncomingRpcRequest {
    public final long   messageId;
    public final UUID from;
    public final FleaseRequestMessage message;

    /**
     * Constructor used by RPC subsystem.
     * @param messageId the request message id.
     * @param from      the other peer we are talking to.
     * @param message   the actual message contents.
     */
    public IncomingRpcRequest(long messageId, UUID from, FleaseRequestMessage message) {
        this.messageId = messageId;
        this.from = from;
        this.message = message;
    }

    public BallotNumber getBallotNumber() {
        return new BallotNumber(message.getK());
    }

    public LeaseValue getLease() {
        return new LeaseValue(message.getLease());
    }
}
