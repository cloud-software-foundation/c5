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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xtreemfs.foundation.LifeCycleThread;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.flease.comm.tcp.TCPConnection.SendRequest;

/**
 *
 * @author bjko
 */
public class TCPCommunicator extends LifeCycleThread {
    private static final Logger LOG = LoggerFactory.getLogger(TCPCommunicator.class);
    private final int port;

    /**
     * the server socket
     */
    private final ServerSocketChannel socket;

    /**
     * Selector for server socket
     */
    private final Selector selector;

    private final NIOServer implementation;

    private final List<TCPConnection> connections;

    private final Queue<TCPConnection> pendingCons;

    private final AtomicInteger        sendQueueSize;

    /**
     *
     * @param implementation
     * @param port, 0 to disable server mode
     * @param bindAddr
     * @throws IOException
     */
    public TCPCommunicator(NIOServer implementation, int port, InetAddress bindAddr) throws IOException {
        super("TCPcom@" + port);

        this.port = port;
        this.implementation = implementation;
        this.connections = new LinkedList();
        this.pendingCons = new ConcurrentLinkedQueue();
        sendQueueSize = new AtomicInteger();

        // open server socket
        if (port == 0) {
            socket = null;
        } else {
            socket = ServerSocketChannel.open();
            socket.configureBlocking(false);
            socket.socket().setReceiveBufferSize(256 * 1024);
            socket.socket().setReuseAddress(true);
            socket.socket().bind(
                    bindAddr == null ? new InetSocketAddress(port) : new InetSocketAddress(bindAddr, port));
        }

        // create a selector and register socket
        selector = Selector.open();
        if (socket != null)
            socket.register(selector, SelectionKey.OP_ACCEPT);
    }

    /**
     * Stop the server and close all connections.
     */
    public void shutdown() {
        this.interrupt();
    }

    /**
     * sends a response.
     *
     */
    public void write(TCPConnection connection, ReusableBuffer buffer, Object context) {
        assert (buffer != null);
        synchronized (connection) {
            if (connection.getChannel().isConnected()) {
                if (connection.sendQueueIsEmpty()) {
                    try {
                        int bytesWritten = connection.getChannel().write(buffer.getBuffer());
                        LOG.debug("directly wrote {} bytes to {}", bytesWritten, connection.getChannel().socket().getRemoteSocketAddress());
                        if (bytesWritten < 0) {
                            if (context != null)
                                implementation.onWriteFailed(new IOException("remote party closed connection while writing"), context);
                            abortConnection(connection,new IOException("remote party closed connection while writing"));
                            return;
                        }
                        if (!buffer.hasRemaining()) {
                            //we are done
                            BufferPool.free(buffer);
                            return;
                        }
                    } catch (ClosedChannelException ex) {
                        if (context != null)
                            implementation.onWriteFailed(ex, context);
                        abortConnection(connection,ex);
                    } catch (IOException ex) {
                        LOG.debug("Exception in write", ex);
                        if (context != null)
                            implementation.onWriteFailed(ex, context);
                        abortConnection(connection,ex);
                    }
                }

                synchronized (connection) {
                    boolean isEmpty = connection.sendQueueIsEmpty();
                    if (isEmpty) {
                        try {
                            SelectionKey key = connection.getChannel().keyFor(selector);
                            if (key != null) {
                                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                            }
                        } catch (CancelledKeyException ex) {
                        }
                    }
                }

                sendQueueSize.incrementAndGet();
                connection.addToSendQueue(new TCPConnection.SendRequest(buffer, context));
                LOG.debug("enqueued write to {}", connection.getEndpoint());
                selector.wakeup();
            } else {
                // ignore and free bufers
                if (connection.getChannel().isConnectionPending()) {
                    sendQueueSize.incrementAndGet();
                    connection.addToSendQueue(new TCPConnection.SendRequest(buffer, context));
                    LOG.debug("enqueued write to {}", connection.getEndpoint());
                } else {
                    BufferPool.free(buffer);
                    if (context != null)
                        implementation.onWriteFailed(new IOException("Connection already closed"), context);
                }
            }
        }
    }

    public void run() {
        notifyStarted();

        LOG.info("TCP Server @{} ready", port);

        try {
            while (!isInterrupted()) {
                // try to select events...
                try {
                    final int numKeys = selector.select();
                    if (!pendingCons.isEmpty()) {
                         while (true) {
                            TCPConnection con = pendingCons.poll();
                            if (con == null) {
                                break;
                            }
                            try {
                                assert(con.getChannel() != null);
                                con.getChannel().register(selector,
                                    SelectionKey.OP_CONNECT | SelectionKey.OP_WRITE | SelectionKey.OP_READ, con);
                            } catch (ClosedChannelException ex) {
                                abortConnection(con,ex);
                            }
                        }
                    }
                    if (numKeys == 0) {
                        continue;
                    }
                } catch (CancelledKeyException ex) {
                    // who cares
                } catch (IOException ex) {
                    LOG.warn("Exception while selecting: {}", ex);
                    continue;
                }

                // fetch events
                Set<SelectionKey> keys = selector.selectedKeys();
                Iterator<SelectionKey> iter = keys.iterator();

                // process all events
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();

                    // remove key from the list
                    iter.remove();
                    try {

                        if (key.isAcceptable()) {
                            acceptConnection(key);
                        }
                        if (key.isConnectable()) {
                            connectConnection(key);
                        }
                        if (key.isReadable()) {
                            readConnection(key);
                        }
                        if (key.isWritable()) {
                            writeConnection(key);
                        }
                    } catch (CancelledKeyException ex) {
                        // nobody cares...
                        continue;
                    }
                }
            }

            for (TCPConnection con : connections) {
                try {
                    con.close(implementation,new IOException("server shutdown"));
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }

            // close socket
            selector.close();
            if (socket != null)
                socket.close();

            LOG.info("TCP Server @{} shutdown complete", port);

            notifyStopped();
        } catch (Throwable thr) {
            LOG.error("TCP Server @{} CRASHED!", port);
            notifyCrashed(thr);
        }
    }

    private void connectConnection(SelectionKey key) {
        final TCPConnection con = (TCPConnection) key.attachment();
        final SocketChannel channel = con.getChannel();

        try {
            if (channel.isConnectionPending()) {
                channel.finishConnect();
            }
            synchronized (con) {
                if (con.getSendBuffer() != null) {
                    key.interestOps(SelectionKey.OP_WRITE | SelectionKey.OP_READ);
                } else {
                    key.interestOps(SelectionKey.OP_READ);
                }
            }
            LOG.debug("connected from {} to {}", con.getChannel().socket().getLocalSocketAddress(),
                channel.socket().getRemoteSocketAddress());
            implementation.onConnect(con.getNIOConnection());
        } catch (IOException ex) {
            LOG.error("connectConnection",ex);
            implementation.onConnectFailed(con.getEndpoint(), ex, con.getNIOConnection().getContext());
            con.close(implementation,ex);
        }

    }

    public NIOConnection connect(InetSocketAddress server, Object context) throws IOException {
        TCPConnection con = openConnection(server,context);
        return con.getNIOConnection();
    }

    private TCPConnection openConnection(InetSocketAddress server, Object context) throws IOException {

        LOG.debug("connect to {}", server);
        SocketChannel channel = null;
        TCPConnection con = null;
        try {
            channel = SocketChannel.open();
            channel.configureBlocking(false);
            //channel.socket().setTcpNoDelay(true);
            channel.socket().setReceiveBufferSize(256 * 1024);
            channel.connect(server);
            con = new TCPConnection(channel, this, server);
            con.getNIOConnection().setContext(context);
            pendingCons.add(con);
            selector.wakeup();
            LOG.debug("connection established");
            return con;
        } catch (IOException ex) {
            LOG.debug("cannot contact server {}", server);
            if (con != null)
                con.close(implementation, ex);
            throw ex;
        }


    }


    /**
     * accept a new incomming connection
     *
     * @param key
     *            the acceptable key
     */
    private void acceptConnection(SelectionKey key) {
        SocketChannel client = null;
        TCPConnection connection = null;
        // FIXME: Better exception handling!

        try {
            // accept connection
            client = socket.accept();
            connection = new TCPConnection(client,this,(InetSocketAddress)client.socket().getRemoteSocketAddress());

            // and configure it to be non blocking
            // IMPORTANT!
            client.configureBlocking(false);
            client.register(selector, SelectionKey.OP_READ, connection);
            //client.socket().setTcpNoDelay(true);

            //numConnections.incrementAndGet();

            connections.add(connection);

            LOG.debug("connect from client at {}", client.socket().getRemoteSocketAddress());
            implementation.onAccept(connection.getNIOConnection());

        } catch (ClosedChannelException ex) {
                LOG.debug("cannot establish connection:", ex);
        } catch (IOException ex) {
            LOG.debug("cannot establish connection: %s", ex);
        }
    }


    /**
     * read data from a readable connection
     *
     * @param key
     *            a readable key
     */
    private void readConnection(SelectionKey key) {
        final TCPConnection con = (TCPConnection) key.attachment();
        final SocketChannel channel = con.getChannel();

        try {

            while (true) {
                final ReusableBuffer readBuf = con.getReceiveBuffer();
                if (readBuf == null)
                    return;
                final int numBytesRead = channel.read(readBuf.getBuffer());
                if (numBytesRead == -1) {
                    // connection closed
                    LOG.debug("client closed connection (EOF): {}", channel.socket().getRemoteSocketAddress());
                    abortConnection(con,new IOException("remote end closed connection while reading data"));
                    return;
                } else if (numBytesRead == 0) {
                    return;
                }
                implementation.onRead(con.getNIOConnection(), readBuf);
            }
        } catch (ClosedChannelException ex) {
            LOG.debug("connection to {} closed by remote peer", con.getChannel().socket().getRemoteSocketAddress());

            abortConnection(con,ex);
        } catch (IOException ex) {
            // simply close the connection
            LOG.error("readConnection", ex);
            abortConnection(con,ex);
        }
    }

    /**
     * write data to a writeable connection
     *
     * @param key
     *            the writable key
     */
    private void writeConnection(SelectionKey key) {
        final TCPConnection con = (TCPConnection) key.attachment();
        final SocketChannel channel = con.getChannel();

        SendRequest srq = null;
        try {

            while (true) {
                srq = con.getSendBuffer();
                if (srq == null) {
                    synchronized (con) {
                        srq = con.getSendBuffer();
                        if (srq == null) {
                            // no more responses, stop writing...
                            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                            return;
                        }
                    }
                }
                // send data
                final long numBytesWritten = channel.write(srq.getData().getBuffer());
                LOG.debug("wrote %d bytes to {}",numBytesWritten,channel.socket().getRemoteSocketAddress());
                if (numBytesWritten == -1) {
                    LOG.info("client closed connection (EOF): {}", channel.socket().getRemoteSocketAddress());
                    // connection closed
                    
                    abortConnection(con, new IOException("remote end closed connection while writing data"));
                    return;
                }
                if (srq.getData().hasRemaining()) {
                    // not enough data...
                    break;
                }
                // finished sending fragment
                // clean up :-) request finished

                BufferPool.free(srq.getData());
                sendQueueSize.decrementAndGet();
                con.nextSendBuffer();
                
            }
        } catch (ClosedChannelException ex) {
            LOG.debug("connection to {} closed by remote peer", con.getChannel().socket().getRemoteSocketAddress());
            abortConnection(con,ex);
        } catch (IOException ex) {
            // simply close the connection
            LOG.error("", ex);
            abortConnection(con,ex);
        }
    }

    public int getSendQueueSize() {
        return sendQueueSize.get();
    }

    void closeConnection(TCPConnection con) {
        if (con.isClosed())
            return;
        con.setClosed();
        final SocketChannel channel = con.getChannel();

        // remove the connection from the selector and close socket
        try {
            synchronized (connections) {
                connections.remove(con);
            }
            final SelectionKey key = channel.keyFor(selector);
            if (key != null)
                key.cancel();
            con.close(null,null);
        } catch (Exception ex) {
        }

        LOG.debug("closing connection to {}", channel.socket().getRemoteSocketAddress());
    }

    void abortConnection(TCPConnection con, IOException exception) {
        if (con.isClosed())
            return;
        con.setClosed();
        final SocketChannel channel = con.getChannel();

        // remove the connection from the selector and close socket
        try {
            synchronized (connections) {
                connections.remove(con);
            }
            final SelectionKey key = channel.keyFor(selector);
            if (key != null)
                key.cancel();
            con.close(implementation,exception);
        } catch (Exception ex) {
        }

        LOG.debug("closing connection to {}", channel.socket().getRemoteSocketAddress());
        implementation.onClose(con.getNIOConnection());
    }

    int getPort() {
        return this.port;
    }
}
