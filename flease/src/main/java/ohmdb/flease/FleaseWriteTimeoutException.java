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
import ohmdb.flease.rpc.IncomingRpcReply;

import java.util.List;

/**
 * Timed out waiting for enough ackWRITE
 */
public class FleaseWriteTimeoutException extends Throwable {
    final List<IncomingRpcReply> replies;
    final int majority;

    public FleaseWriteTimeoutException(List<IncomingRpcReply> replies, int majority) {
        this.replies = ImmutableList.copyOf(replies);
        this.majority = majority;
    }

    @Override
    public String toString() {
        StringBuilder sb =  new StringBuilder();
        sb.append("Lease write timed out, we got replies from ").append(replies.size()).append(" peers, majority needed: ").append(majority);
        sb.append(", peers we got replies from were: \n ");
        for( IncomingRpcReply reply : replies) {
            sb.append(reply.from);
            sb.append(" ");
        }

        return sb.toString();
    }

}
