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
package org.xtreemfs.foundation.flease.acceptor;

import java.io.Serializable;

import org.xtreemfs.foundation.flease.comm.FleaseMessage;

/**
 *
 * @author bjko
 */
public class FleaseInstance implements Serializable {
    
    /** Creates a new instance of PxInstance */
    public FleaseInstance() {
        learned = false;
        timeout = 0;
        prepared = new FleaseMessage(FleaseMessage.MsgType.MSG_PREPARE);
        accepted = null;
        
    }

    /**
     * Holds value of property learned.
     */
    private boolean learned;

    /**
     * Getter for property learned.
     * @return Value of property learned.
     */
    public boolean isLearned() {
        return this.learned;
    }

    /**
     * Setter for property learned.
     * @param learned New value of property learned.
     */
    public void setLearned(boolean learned) {
        this.learned = learned;
    }

    /**
     * Holds value of property timeout.
     */
    private long timeout;

    /**
     * Getter for property timeout.
     * @return Value of property timeout.
     */
    public long getTimeout() {
        return this.timeout;
    }

    /**
     * Setter for property timeout.
     * @param timeout New value of property timeout.
     */
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    /**
     * Holds value of property prepared.
     */
    private FleaseMessage prepared;

    /**
     * Getter for property lastPrep.
     * @return Value of property lastPrep.
     */
    public FleaseMessage getPrepared() {
        return this.prepared;
    }

    /**
     * Setter for property lastPrep.
     * @param prepared New value of property lastPrep.
     */
    public void setPrepared(FleaseMessage prepared) {
        this.prepared = prepared;
    }

    /**
     * Holds value of property accepted.
     */
    private FleaseMessage accepted;

    /**
     * Getter for property accepted.
     * @return Value of property accepted.
     */
    public FleaseMessage getAccepted() {
        return this.accepted;
    }

    /**
     * Setter for property accepted.
     * @param accepted New value of property accepted.
     */
    public void setAccepted(FleaseMessage accepted) {
        assert(accepted.getLeaseHolder() != null);
        assert(accepted.getLeaseTimeout() > 0);
        this.accepted = accepted;
    }
    
}
