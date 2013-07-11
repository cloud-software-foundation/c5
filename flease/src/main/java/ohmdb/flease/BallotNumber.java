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

/**
 * A ballot number in the flease system.  Unwraps and serializes to protobufs.
 * Implements comparable.
 *
 * This object is immutable.
 */
public class BallotNumber implements Comparable<BallotNumber> {

    // normally 'time_t' in milliseconds
    private final long ballotNumber;
    private final long messageNumber;
    private final long processId;
    // maybe stashed from constructor.
    private final Flease.BallotNumber protobuf;

    /**
     * The "zero" constructor, that has every field set to 0.
     */
    public BallotNumber() {
        this(0, 0, 0);
    }

    public BallotNumber(long ballotNumber, long messageNumber, long processId) {
        this.ballotNumber = ballotNumber;
        this.messageNumber = messageNumber;
        this.processId = processId;
        this.protobuf = null;
    }

    public BallotNumber(Flease.BallotNumber fromMessage) {
        this.ballotNumber = fromMessage.getBallotNumber();
        this.messageNumber = fromMessage.getMessageNumber();
        this.processId = fromMessage.getId();
        this.protobuf = fromMessage;
    }

    /**
     * Builds the protobuf message for this object.  Nominally this creates a new
     * protobuf message every call, so don't go too crazy.
     *
     * As an implementation detail, this might return a previously constructed protobuf message,
     * for example if this object was created from a protobuf message, but that's a minor perf detail
     * that one should not rely on.
     *
     * @return protobuf message for this object.
     */
    public Flease.BallotNumber getMessage() {
        // Thanks to immutable objects, lets do this.
        if (protobuf != null) return protobuf;

        // YES, in theory we could update protobuf but it would be problematic for a few reasons:
        // * synchronization becomes an issue
        // * we'd have to get rid of that final, which I like the way it is.
        // So yeah, just dont call getMessage() too much.
        Flease.BallotNumber.Builder builder = Flease.BallotNumber.newBuilder();
        builder.setBallotNumber(ballotNumber)
                .setMessageNumber(messageNumber)
                .setId(processId);
        return builder.build();
    }

    @Override
    public String toString() {
        return ballotNumber + "/" + messageNumber + "@" + processId;
    }

    @Override
    public int compareTo(BallotNumber o) {
        if (o == null) {
            // this.compareTo(null) => this > null, we are greater than.
            return 1;
        }

        // TODO compare using interval, message number and processId.
        if (ballotNumber == o.ballotNumber) {
            return (int)(messageNumber - o.messageNumber);
        } else if (ballotNumber > o.ballotNumber)
            return 1;
        else
            return -1;
    }

    public long compareTo(BallotNumber o, InformationInterface info) {
        if (o == null) {
            return 1;
        }

        if (interval(info) == o.interval(info)) {
            if (messageNumber == o.messageNumber) {
                return processId - o.processId;
            }
            return messageNumber - o.messageNumber;
        } else {
            return interval(info) - o.interval(info);
        }
    }

    public long interval(InformationInterface info) {
        return ballotNumber / (info.getLeaseLength() - info.getEpsilon());
    }


    @Override
    public int hashCode() {
        return ((int)(ballotNumber ^ (ballotNumber >>> 32)))
                ^ ((int)(messageNumber ^ (messageNumber >>> 32)))
                ^ ((int)(processId ^ (processId >>> 32)));
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) return true;
        if (other == null) return false;

        if (other instanceof BallotNumber) {
            BallotNumber bOther = (BallotNumber)other;

            if (ballotNumber == bOther.ballotNumber
                    && messageNumber == bOther.messageNumber
                    && processId == bOther.processId)
                return true;

        }
        return false;
    }
}
