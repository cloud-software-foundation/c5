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
 * Copyright (c) 2009-2010 by Bjoern Kolbeck, Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package org.xtreemfs.foundation.flease.comm.tcp;

import java.net.InetSocketAddress;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

/**
 *
 * @author bjko
 */
public class NIOConnection {

    private TCPConnection connection;

    private Object        context;

    public NIOConnection(TCPConnection connection) {
        this.connection = connection;
    }

    public void setContext(Object context) {
        this.context = context;
    }

    public Object getContext() {
        return context;
    }

    /**
     * READ MUST ONLY BE USED FROM A NIOServer callback!
     * @param buffer
     */
    public void read(ReusableBuffer buffer) {
        connection.setReceiveBuffer(buffer);
    }

    /**
     * Write is thread safe
     * @param buffer
     */
    public void write(ReusableBuffer buffer, Object context) {
        connection.getServer().write(connection, buffer, context);
        //FIXME:wakeup selector, change interest set
    }

    public void close() {
        connection.getServer().closeConnection(connection);
    }

    public InetSocketAddress getEndpoint() {
        return connection.getEndpoint();
    }

    public String toString() {
        return "TCP connection (from "+connection.getChannel().socket().getRemoteSocketAddress()+" to local server @ "+connection.getServer().getPort()+")";
    }

}
