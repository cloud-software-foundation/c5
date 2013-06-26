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
import java.util.concurrent.TimeUnit;

import static ohmdb.flease.Flease.FleaseRequestMessage;

/**
 * Used to send RPC requests from the lease fiber.
 *
 * Envelope data added by RPC system.
 */
public class OutgoingRpcRequest {

    public final FleaseRequestMessage message;
    public final UUID to;
    public final UUID from;

    // The RPC system may support retries, if so it will do according to these params.
    public final int retries;
    public final int retrySpacing;
    public final TimeUnit timeUnit;

    public OutgoingRpcRequest(UUID from, FleaseRequestMessage message, UUID to,
                              int retries, int retrySpacing, TimeUnit timeUnit) {
        this.retries = retries;
        this.retrySpacing = retrySpacing;
        this.timeUnit = timeUnit;

        this.from = from;
        this.message = message;
        this.to = to;
    }

    /**
     * Default constructor.  Set retries to '10', retry spacing to '2', timeUnit = SECONDS.
     * @param message
     * @param to
     */
    public OutgoingRpcRequest(UUID from, FleaseRequestMessage message, UUID to) {
        this(from, message, to,
                10,
                2,
                TimeUnit.SECONDS);
    }

    public BallotNumber getBallotNumber() {
        return new BallotNumber(message.getK());
    }

    public Flease.Lease getLeaseData() {
        return message.getLease();
    }
}
