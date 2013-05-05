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

package org.xtreemfs.foundation.flease.sim;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xtreemfs.foundation.TimeSync;
import org.xtreemfs.foundation.buffer.ASCIIString;
import org.xtreemfs.foundation.flease.Flease;
import org.xtreemfs.foundation.flease.FleaseConfig;
import org.xtreemfs.foundation.flease.FleaseMessageSenderInterface;
import org.xtreemfs.foundation.flease.FleaseStage;
import org.xtreemfs.foundation.flease.FleaseStatusListener;
import org.xtreemfs.foundation.flease.FleaseViewChangeListenerInterface;
import org.xtreemfs.foundation.flease.MasterEpochHandlerInterface;
import org.xtreemfs.foundation.flease.comm.FleaseMessage;
import org.xtreemfs.foundation.flease.proposer.FleaseException;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Simulator for testing
 * @author bjko
 */
public class FleaseSim {
  private static final Logger LOG = LoggerFactory.getLogger(FleaseSim.class);

    //private void

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            final int dmax = 500;
            final int leaseTimeout = 5000;

            final int numHosts = 10;
            final FleaseStage[] stages = new FleaseStage[numHosts];

            final boolean useME = true;

            TimeSync.initializeLocal(50);

            final Communicator com = new Communicator(10, 100, 30000, 5, true, 0.2, 0.05);
            if (com.startAndWait() != Service.State.RUNNING) {
                LOG.error("Unable to start communicator", com.failureCause());
                System.exit(2);
            }

            List<InetSocketAddress> allPorts = new ArrayList(numHosts);
            List<InetSocketAddress>[] acceptors = new List[numHosts];
            MasterEpochHandlerInterface[] meHandlers = new MasterEpochHandlerInterface[numHosts];
            final AtomicReference<Flease>[] leaseStates = new AtomicReference[numHosts];

            for (int i = 0; i < numHosts; i++) {
                final int portNo = 1024+i;
                final int myI = i;
                FleaseConfig cfg = new FleaseConfig(leaseTimeout, dmax, 500, new InetSocketAddress(portNo), "localhost:"+(1024+i),5, true, 0);
                leaseStates[i] = new AtomicReference<Flease>(Flease.EMPTY_LEASE);

                if (useME) {
                    meHandlers[i] = new MasterEpochHandlerInterface() {
                        long me = 0;

                        public long getMasterEpoch() {
                            return me;
                        }

                        @Override
                        public void sendMasterEpoch(FleaseMessage response, Continuation callback) {
                            response.setMasterEpochNumber(me);
                            callback.processingFinished();
                        }

                        @Override
                        public void storeMasterEpoch(FleaseMessage request, Continuation callback) {
                            me = request.getMasterEpochNumber();
                            callback.processingFinished();
                        }
                    };
                }


                stages[i] = new FleaseStage(cfg, "/tmp/xtreemfs-test", new FleaseMessageSenderInterface() {

                    public void sendMessage(FleaseMessage message, InetSocketAddress recipient) {
                        assert(message != null);
                        LOG.debug("received message for delivery to port {}: {}",recipient.getPort(),message);
                        message.setSender(new InetSocketAddress("localhost", portNo));
                        com.send(recipient.getPort(), message);
                    }
                }, true, new FleaseViewChangeListenerInterface() {

                    public void viewIdChangeEvent(ASCIIString cellId, int viewId) {
                    }
                },new FleaseStatusListener() {

                    public void statusChanged(ASCIIString cellId, Flease lease) {

                        synchronized (leaseStates) {
                            leaseStates[myI].set(lease);
                        }
                        System.out.println("state change for "+portNo+": "+lease.getLeaseHolder()+"/"+lease.getLeaseTimeout_ms());
                    }

                    public void leaseFailed(ASCIIString cellId, FleaseException error) {
                        System.out.println("lease failed: "+error);
                        synchronized (leaseStates) {
                            leaseStates[myI].set(Flease.EMPTY_LEASE);
                        }
                        //restart my cell
                    }
                },meHandlers[i]);
                stages[i].addListener(new Service.Listener() {
                    @Override
                    public void starting() {
                    }

                    @Override
                    public void running() {
                    }

                    @Override
                    public void stopping(Service.State from) {
                    }

                    @Override
                    public void terminated(Service.State from) {
                    }

                    @Override
                    public void failed(Service.State from, Throwable failure) {
                        LOG.error("Failed service, from state: {} with exception {}", from, failure);
                        System.exit(100);
                    }
                }, MoreExecutors.sameThreadExecutor());
                stages[i].start();
                allPorts.add(new InetSocketAddress("localhost", 1024+i));
                com.openPort(1024+i, stages[i]);
            }

            for (int i = 0; i < numHosts; i++) {
                final int portNo = 1024+i;
                acceptors[i] = new ArrayList(numHosts-1);
                for (InetSocketAddress ia : allPorts) {
                    if (ia.getPort() != portNo)
                        acceptors[i].add(ia);
                }
                stages[i].openCell(new ASCIIString("testcell"), acceptors[i],false);
            }

            //do something

            do {
                final AtomicBoolean sync = new AtomicBoolean();
                final AtomicReference<ASCIIString> ref = new AtomicReference();

                Thread.sleep(100);
                System.out.print("checking local states: ");
                ASCIIString leaseHolder = null;
                int    leaseInstanceId = 0;



                synchronized (leaseStates) {
                    for (int i = 0; i < numHosts; i++) {
                        if (!leaseStates[i].get().isEmptyLease()) {
                            if (leaseHolder == null) {
                                leaseHolder = leaseStates[i].get().getLeaseHolder();
                            } else {
                                if (!leaseHolder.equals(leaseStates[i].get().getLeaseHolder())) {
                                    com.stop();
                                    System.out.println("INVARIANT VIOLATED!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                                    System.out.println("\n\n");
                                    System.out.println("got lease for: "+leaseHolder);
                                    System.out.println("but host "+i+" learned "+leaseStates[i].get().getLeaseHolder());

                                    for (int j = 0; j < numHosts; j++) {
                                        System.out.println(stages[j]._dump_acceptor_state(new ASCIIString("testcell")));
                                        System.out.println("signalled result: "+leaseStates[j].get());
                                        System.out.println("          valid: "+leaseStates[j].get().isValid());
                                    }
                                    System.exit(2);
                                } else {
                                    System.out.print("+");
                                }
                            }
                        } else {
                            System.out.print("o");
                        }
                    }
                }

                System.out.println("");

                final int host = (int)(Math.random()*numHosts);
                stages[host].closeCell(new ASCIIString("testcell"), false);


                //make sure that cells also time out some times (requires a wait > 2*lease_timeout)
                final int waitTime = (int)(Math.random()*(leaseTimeout*2+1000));
                System.out.println("waiting for "+waitTime+"ms");
                Thread.sleep(waitTime);
                stages[host].openCell(new ASCIIString("testcell"), acceptors[host],false);
            } while (true);

            /*
            for (int i = 0; i < numHosts; i++) {
                stages[i].shutdown();
            }
            com.shutdown();
             */


        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }

    }

}
