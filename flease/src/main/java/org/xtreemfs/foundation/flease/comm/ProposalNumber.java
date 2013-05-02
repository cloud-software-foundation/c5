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
 *
 *  This file incorporates work covered by the following copyright and
 *  permission notice:
 */

/*
 * Copyright (c) 2009-2011 by Bjoern Kolbeck, Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package org.xtreemfs.foundation.flease.comm;

import org.xtreemfs.foundation.buffer.ReusableBuffer;


/**
 *
 * @author bjko
 */
public class ProposalNumber implements Cloneable {

    private final long  proposalNo;
    private final long  senderId;

    public static final ProposalNumber EMPTY_PROPOSAL_NUMBER = new ProposalNumber(0, 0);

    public ProposalNumber(long proposalNo, long senderId) {
        this.proposalNo = proposalNo;
        this.senderId = senderId;
    }

    public ProposalNumber(ReusableBuffer buffer) {
        this.proposalNo = buffer.getLong();
        this.senderId = buffer.getLong();
    }

    /**
     * checks if this message is before other
     * @param other another message
     * @return true if this message is before other
     */
    public boolean before(ProposalNumber other) {
        if (this.proposalNo < other.proposalNo) {
            return true;
        } else if (this.proposalNo > other.proposalNo) {
            return false;
        } else {
            return (this.senderId < other.senderId);
        }
    }

    /**
     * checks if this message is after other
     * @param other another message
     * @return true if this message is after (i.e. >) other
     */
    public boolean after(ProposalNumber other) {
        if (this.proposalNo > other.proposalNo) {
            return true;
        } else if (this.proposalNo < other.proposalNo) {
            return false;
        } else {
            return (this.senderId > other.senderId);
        }
    }

    public boolean sameNumber(ProposalNumber other) {
        return (senderId == other.senderId)&& (proposalNo == other.proposalNo);
    }

    public boolean isEmpty() {
        if (this == EMPTY_PROPOSAL_NUMBER)
            return true;
        return this.sameNumber(EMPTY_PROPOSAL_NUMBER);
    }

    public void serialize(ReusableBuffer buffer) {
        buffer.putLong(this.proposalNo);
        buffer.putLong(this.senderId);

    }

    /**
     * @return the proposalNo
     */
    public long getProposalNo() {
        return proposalNo;
    }

    /**
     * @return the senderId
     */
    public long getSenderId() {
        return senderId;
    }

    public String toString() {
        return "("+proposalNo+";"+senderId+")";
    }


}
