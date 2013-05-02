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

import org.xtreemfs.foundation.buffer.ASCIIString;
import org.xtreemfs.foundation.flease.proposer.FleaseException;
import org.xtreemfs.foundation.flease.proposer.FleaseListener;

/**
 *
 * @author bjko
 */
public class FleaseFuture implements FleaseListener {

    private volatile Flease result;

    private volatile FleaseException error;

    FleaseFuture() {
        result = null;
    }

    public Flease get() throws FleaseException,InterruptedException {

        synchronized (this) {
            if ((result == null) && (error == null))
                this.wait();

            if (error != null)
                throw error;

            return result;
        }
        
    }
    public void proposalResult(ASCIIString cellId, ASCIIString leaseHolder, long leaseTimeout_ms, long masterEpochNumber) {
        synchronized (this) {
            result = new Flease(cellId,leaseHolder,leaseTimeout_ms,masterEpochNumber);
            this.notifyAll();
        }
    }

    public void proposalFailed(ASCIIString cellId, Throwable cause) {
        synchronized (this) {
            if (cause instanceof FleaseException) {
                error = (FleaseException) cause;
            } else {
                error = new FleaseException(cause.getMessage(), cause);
            }
            this.notifyAll();
        }
    }

}
