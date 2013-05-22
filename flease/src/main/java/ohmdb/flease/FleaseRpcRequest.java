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

import static ohmdb.flease.Flease.FleaseRequestMessage;

/**
 * Used to send RPC requests from the lease fiber.
 */
public class FleaseRpcRequest extends RetriedMessage {

    public final FleaseRequestMessage message;
    public final InetSocketAddress remoteAddress;

    /**
     * Default constructor.  Set retries to '10', retry spacing to '2', timeUnit = SECONDS.
     * @param message
     * @param remoteAddress
     */
    public FleaseRpcRequest(FleaseRequestMessage message, InetSocketAddress remoteAddress) {
        this(message, remoteAddress,
                10,
                2,
                TimeUnit.SECONDS);
    }

    public FleaseRpcRequest(FleaseRequestMessage message, InetSocketAddress remoteAddress,
                            int retries, int retrySpacing, TimeUnit timeUnit) {
        super(retries, retrySpacing, timeUnit);

        this.message = message;
        this.remoteAddress = remoteAddress;
    }

    public BallotNumber getBallotNumber() {
        return new BallotNumber(message.getK());
    }

    public Flease.Lease getLeaseData() {
        return message.getLease();
    }
}
