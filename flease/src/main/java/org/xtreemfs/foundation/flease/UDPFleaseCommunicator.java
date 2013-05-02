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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xtreemfs.foundation.LifeCycleThread;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.flease.comm.FleaseMessage;

/**
 *
 * @author bjko
 */
public class UDPFleaseCommunicator extends LifeCycleThread implements FleaseMessageSenderInterface {
    private static final Logger LOG = LoggerFactory.getLogger(UDPFleaseCommunicator.class);
    
    private final FleaseStage                     stage;

    private final int                             port;

    private DatagramChannel                       channel;

    private Selector                              selector;

    private volatile boolean                      quit;

    private final AtomicBoolean                   sendMode;

    private final LinkedBlockingQueue<FleaseMessage> q;

    private static final int                      MAX_UDP_SIZE  = 16*1024;

    private long numTx,numRx;

    public UDPFleaseCommunicator(FleaseConfig config, String lockfileDir,
            boolean ignoreLockForTesting,
            final FleaseViewChangeListenerInterface viewListener) throws Exception {
        super("FlUDPCom");
        stage = new FleaseStage(config, lockfileDir, this, ignoreLockForTesting, viewListener, null, null);
        port = config.getEndpoint().getPort();
        q = new LinkedBlockingQueue<FleaseMessage>();
        sendMode = new AtomicBoolean(false);
        numTx = 0;
        numRx = 0;
    }

    public FleaseStage getStage() {
        return stage;
    }


    public void sendMessage(FleaseMessage message, InetSocketAddress recipient) {
        FleaseMessage m = message.clone();
        m.setSender(recipient);
        send(m);
    }

    /**
     * sends a UDPRequest.
     *
     * @attention Overwrites the first byte of rq.data with the message type.
     */
    public void send(FleaseMessage rq) {
        q.add(rq);

        if (q.size() == 1) {
            //System.out.println("wakeup!");
            selector.wakeup();
        }
    }

    public void shutdown() {
        quit = true;
        interrupt();
    }

    @Override
    public void run() {

        long numRxCycles = 0;
        long durAllRx = 0;
        long maxPkgCycle = 0;
        long avgPkgCycle = 0;

        try {


            selector = Selector.open();

            channel = DatagramChannel.open();
            channel.socket().bind(new InetSocketAddress(port));
            channel.socket().setReceiveBufferSize(1024*1024*1024);
            channel.socket().setSendBufferSize(1024*1024*1024);
            LOG.debug("sendbuffer size: {}", channel.socket().getSendBufferSize());
            LOG.debug("recv       size: {}", channel.socket().getReceiveBufferSize());
            channel.configureBlocking(false);
            channel.register(selector, SelectionKey.OP_READ);

            LOG.info("UDP socket on port {} ready", port);

            stage.start();
            stage.waitForStartup();


            notifyStarted();


            boolean isRdOnly = true;

            List<FleaseMessage> sendList = new ArrayList(5000);
            ReusableBuffer data = BufferPool.allocate(MAX_UDP_SIZE);

            while (!quit) {

                if (q.size() == 0) {
                    if (!isRdOnly) {
                        channel.keyFor(selector).interestOps(SelectionKey.OP_READ);
                        //System.out.println("read only");
                        isRdOnly = true;
                    }
                } else {
                    if (isRdOnly) {
                        channel.keyFor(selector).interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                        //System.out.println("read write");
                        isRdOnly = false;
                    }
                }

                int numKeys = selector.select();

                if (q.size() == 0) {
                    if (!isRdOnly) {
                        channel.keyFor(selector).interestOps(SelectionKey.OP_READ);
                        //System.out.println("read only");
                        isRdOnly = true;
                    }
                } else {
                    if (isRdOnly) {
                        channel.keyFor(selector).interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                        //System.out.println("read write");
                        isRdOnly = false;
                    }
                }

                if (numKeys == 0)
                    continue;

                if (q.size() > 10000) {
                    System.out.println("QS!!!!! " + q.size());
                    System.out.println("is readOnly: " + isRdOnly);
                }

                // fetch events
                Set<SelectionKey> keys = selector.selectedKeys();
                Iterator<SelectionKey> iter = keys.iterator();

                // process all events
                while (iter.hasNext()) {

                    SelectionKey key = iter.next();

                    // remove key from the list
                    iter.remove();

                    if (key.isWritable()) {
                        q.drainTo(sendList,50);
                        //System.out.println("sent: "+queue.size());
                        while (!sendList.isEmpty()) {
                            FleaseMessage r = sendList.remove(sendList.size()-1);
                            if (r == null)
                                break;
                            LOG.debug("sent packet to {}", r.getSender());
                            data.clear();
                            r.serialize(data);
                            data.flip();
                            int sent = channel.send(data.getBuffer(), r.getSender());
                            if (sent == 0) {
                                System.out.println("cannot send anymore!");
                                q.addAll(sendList);
                                sendList.clear();
                                break;
                            }
                            numTx++;
                        }
                    }
                    if (key.isReadable()) {
                        InetSocketAddress sender = null;

                        int numRxInCycle = 0;
                        do {
                            data.clear();
                            sender = (InetSocketAddress) channel.receive(data.getBuffer());
                            if (sender == null) {
                                LOG.debug("read key for empty read");
                                break;
                            } else {
                                numRxInCycle++;
                                LOG.debug("read data from {}", sender);

                                try {
                                    //unpack flease message
                                    data.flip();
                                    FleaseMessage m = new FleaseMessage(data);
                                    m.setSender(sender);
                                    numRx++;
                                    stage.receiveMessage(m);
                                } catch (Throwable ex) {
                                    LOG.warn("received invalid UDP message: ", ex);
                                }
                            }
                        } while (sender != null);
                        numRxCycles++;
                        avgPkgCycle += numRxInCycle;
                        if (numRxInCycle > maxPkgCycle)
                            maxPkgCycle = numRxInCycle;
                    }
                }

            }

            stage.shutdown();

            selector.close();
            channel.close();

        } catch (ClosedByInterruptException ex) {
            // ignore
        } catch (IOException ex) {
            LOG.error("main loop exception", ex);
        } catch (Throwable th) {
            notifyCrashed(th);
            return;
        }


        LOG.info("num packets tranferred: {} tx    {} rx", numTx, numRx);
        LOG.info("numRxCycles {}, maxPkgPerCycle {}, avg/Cycle {}", numRxCycles, maxPkgCycle, avgPkgCycle/numRxCycles);

        notifyStopped();
    }
}
