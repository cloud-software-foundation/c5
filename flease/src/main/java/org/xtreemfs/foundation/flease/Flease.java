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

package org.xtreemfs.foundation.flease;

import org.xtreemfs.foundation.TimeSync;
import org.xtreemfs.foundation.buffer.ASCIIString;
import org.xtreemfs.foundation.flease.comm.FleaseMessage;

/**
 * Object represents a lease.
 * @author bjko
 */
public class Flease {

    public static final Flease EMPTY_LEASE = new Flease(null,null, 0,FleaseMessage.IGNORE_MASTER_EPOCH);

    private final ASCIIString leaseHolder;
    private final long leaseTimeout_ms;
    private final ASCIIString cellId;
    private final long masterEpochNumber;


    public Flease(ASCIIString cellId, ASCIIString leaseHolder, long leaseTimeout_ms, long masterEpochNumber) {
        this.cellId = cellId;
        this.leaseHolder = leaseHolder;
        this.leaseTimeout_ms = leaseTimeout_ms;
        this.masterEpochNumber = masterEpochNumber;
    }

    /**
     * @return the leaseHolder
     */
    public ASCIIString getLeaseHolder() {
        return leaseHolder;
    }

    /**
     * @return the leaseTimeout_ms
     */
    public long getLeaseTimeout_ms() {
        return leaseTimeout_ms;
    }

    public boolean isValid() {
        return (TimeSync.getGlobalTime() < leaseTimeout_ms);
    }

    public boolean isEmptyLease() {
        return leaseTimeout_ms == 0;
    }

    public boolean equals(Object other) {
        try {
            Flease o = (Flease) other;

            boolean sameTo = o.leaseTimeout_ms == this.leaseTimeout_ms;
            return isSameLeaseHolder(o) && sameTo;

        } catch (ClassCastException ex) {
            return false;
        }
    }

    public boolean isSameLeaseHolder(Flease o) {
        return (o.leaseHolder == this.leaseHolder) ||
                    (o.leaseHolder != null) && (this.leaseHolder != null) && (o.leaseHolder.equals(this.leaseHolder));
    }

    /**
     * @return the cellId
     */
    ASCIIString getCellId() {
        return cellId;
    }

    public String toString() {
        return (cellId == null ? "" : cellId.toString())+": "+
                (leaseHolder == null ? "null" : leaseHolder.toString())+"/"+
                leaseTimeout_ms;
    }

    /**
     * @return the masterEpochNumber
     */
    public long getMasterEpochNumber() {
        return masterEpochNumber;
    }
}
