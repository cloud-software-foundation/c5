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

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xtreemfs.foundation.LRUCache;
import org.xtreemfs.foundation.buffer.ASCIIString;
import org.xtreemfs.foundation.flease.comm.FleaseMessage;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 * @author bjko
 */
public class SimpleMasterEpochHandler extends AbstractExecutionThreadService implements MasterEpochHandlerInterface {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleMasterEpochHandler.class);

    public static final int MAX_EPOCH_CACHE_SIZE = 10000;


    private final LinkedBlockingQueue<Request> requests;

    private final LRUCache<ASCIIString,Long>   epochs;

    private final String                       directory;

    public SimpleMasterEpochHandler(String directory) {
        super();
        requests = new LinkedBlockingQueue<Request>();
        epochs = new LRUCache<ASCIIString, Long>(MAX_EPOCH_CACHE_SIZE);
        this.directory = directory;
    }


    @Override
    protected String serviceName() {
        return "SimpleMasterEpochHandler";
    }

    // This seems unnecessary but I'm not sure.
    private Thread executionThread;

    @Override
    protected void startUp() throws Exception {
        // cache this for triggerShutdown.
        executionThread = Thread.currentThread();
    }

    @Override
    protected void triggerShutdown() {
        executionThread.interrupt();
    }

    @Override
    public void run() {
        do {
            Request rq;

            try {
                rq = requests.take();
            } catch (InterruptedException e) {
                if (!isRunning())
                    return;
                else
                    continue;
            }

            switch (rq.type) {
                case SEND: {
                    try {
                        Long epoch = epochs.get(rq.message.getCellId());
                        if (epoch == null) {

                            File f = new File(getFileName(rq.message.getCellId()));
                            if (f.exists()) {
                                RandomAccessFile raf = new RandomAccessFile(f, "r");
                                epoch = raf.readLong();
                                raf.close();
                            } else {
                                epoch = 0l;
                            }
                            epochs.put(rq.message.getCellId(),epoch);
                            LOG.debug("sent {}", epoch);
                        }
                        rq.message.setMasterEpochNumber(epoch);
                    } catch (Exception ex) {
                        rq.message.setMasterEpochNumber(-1);
                    } finally {
                        try {
                            rq.callback.processingFinished();
                        } catch (Exception ex) {
                            LOG.error("exception for SEND, calling rq.callback.processingFinished", ex);
                        }
                    }
                    break;
                }
                case STORE: {
                    try {
                        File f = new File(getFileName(rq.message.getCellId()));
                        RandomAccessFile raf = new RandomAccessFile(f, "rw");
                        raf.writeLong(rq.message.getMasterEpochNumber());
                        raf.getFD().sync();
                        raf.close();
                        LOG.debug("stored {}", rq.message.getMasterEpochNumber());
                        epochs.put(rq.message.getCellId(),rq.message.getMasterEpochNumber());
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    } finally {
                        try {
                            rq.callback.processingFinished();
                        } catch (Exception ex) {
                            LOG.error("exception for STORE, calling rq.callback.processingFinished", ex);
                        }
                    }
                    break;
                }
            }

        } while (isRunning());
    }

    private String getFileName(ASCIIString cellId) {
        return directory + cellId.toString() + ".me";
    }

    @Override
    public void sendMasterEpoch(FleaseMessage response, Continuation callback) {
        Request rq = new Request();
        rq.callback = callback;
        rq.message = response;
        rq.type = Request.RequestType.SEND;
        requests.add(rq);
    }

    @Override
    public void storeMasterEpoch(FleaseMessage request, Continuation callback) {
        Request rq = new Request();
        rq.callback = callback;
        rq.message = request;
        rq.type = Request.RequestType.STORE;
        requests.add(rq);
    }

    private final static class Request {
        FleaseMessage message;
        Continuation callback;
        //String fileName;
        enum RequestType {
            SEND, STORE
        }
        RequestType  type;
    }
}
