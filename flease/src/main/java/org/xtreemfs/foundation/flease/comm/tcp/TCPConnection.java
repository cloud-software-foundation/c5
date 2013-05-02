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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

/**
 *
 * @author bjko
 */
public class TCPConnection {

    private SocketChannel channel;

    private Queue<SendRequest> sendQueue;

    private ReusableBuffer        receiveBuffer;

    private final NIOConnection         nioCon;

    private final TCPCommunicator       myServer;

    private final AtomicBoolean         closed;

    private final InetSocketAddress endpoint;

    public TCPConnection(SocketChannel channel, TCPCommunicator myServer, InetSocketAddress endpoint) {
        this.channel = channel;
        sendQueue = new ConcurrentLinkedQueue<SendRequest>();
        nioCon = new NIOConnection(this);
        this.myServer = myServer;
        closed = new AtomicBoolean(false);
        this.endpoint = endpoint;
    }


    public TCPCommunicator getServer() {
        return myServer;
    }

    public NIOConnection getNIOConnection() {
        return nioCon;
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public InetSocketAddress getEndpoint() {
        return this.endpoint;
    }

    public SendRequest getSendBuffer() {
        return sendQueue.peek();
    }

    public void nextSendBuffer() {
        sendQueue.poll();
    }

    public void addToSendQueue(SendRequest buffer) {
        sendQueue.add(buffer);
    }
    
    public boolean sendQueueIsEmpty() {
        return sendQueue.isEmpty();
    }

    public ReusableBuffer getReceiveBuffer() {
        return receiveBuffer;
    }

    public void setReceiveBuffer(ReusableBuffer buffer) {
        receiveBuffer = buffer;
    }

    public boolean isClosed() {
        return closed.get();
    }

    public void setClosed() {
        closed.set(true);
    }

    public void close(NIOServer implementation, IOException error) {
        try {
            BufferPool.free(receiveBuffer);
            channel.close();
        } catch (IOException ex) {
            //ignore
        } finally {
            for (SendRequest rq : sendQueue) {
                BufferPool.free(rq.data);
            }
            if (implementation != null) {
                for (SendRequest rq : sendQueue) {
                    if (rq.getContext() != null)
                        implementation.onWriteFailed(error, rq.getContext());
                }
            }
        }
    }

    public static class SendRequest {
        private final ReusableBuffer data;
        private final Object context;

        public SendRequest(ReusableBuffer data, Object context) {
            this.data = data;
            this.context = context;
        }

        /**
         * @return the data
         */
        public ReusableBuffer getData() {
            return data;
        }

        /**
         * @return the context
         */
        public Object getContext() {
            return context;
        }
    }

}
