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
 * Copyright (c) 2010 by Felix Langner,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */

package org.xtreemfs.foundation;

import java.net.InetSocketAddress;
import org.xtreemfs.foundation.pbrpc.client.RPCResponse;


/**
 * Provides methods to synchronize with a XtreemFS TimeServer, usually provided
 * by a DIR service.
 * 
 * @author flangner
 * @since 03/01/2010
 */

public interface TimeServerClient {

    /**
     * Requests the global time at the given server.
     * 
     * @param server - if null, the default will be used.
     * @return a {@link RPCResponse} future for an UNIX time-stamp.
     */
    public long xtreemfs_global_time_get(InetSocketAddress server);
}