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

package org.xtreemfs.foundation.flease.proposer;

import junit.framework.TestCase;
import org.xtreemfs.foundation.TimeSync;
import org.xtreemfs.foundation.buffer.ASCIIString;
import org.xtreemfs.foundation.flease.Flease;
import org.xtreemfs.foundation.flease.FleaseConfig;
import org.xtreemfs.foundation.flease.FleaseStatusListener;
import org.xtreemfs.foundation.flease.acceptor.FleaseAcceptor;
import org.xtreemfs.foundation.flease.acceptor.LearnEventListener;
import org.xtreemfs.foundation.flease.comm.FleaseCommunicationInterface;
import org.xtreemfs.foundation.flease.comm.FleaseMessage;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author bjko
 */
public class FleaseProposerTest extends TestCase {

    private FleaseAcceptor acceptor;
    private FleaseProposer proposer;
    private final static ASCIIString TESTCELL = new ASCIIString("testcell");

    private final FleaseConfig cfg;

    private final AtomicReference<Flease> result;

    public FleaseProposerTest(String testName) throws FleaseException {
        super(testName);

        result = new AtomicReference();
        //Logging.start(Logging.LEVEL_WARN, Category.all);
        TimeSync.initializeLocal(50);

        cfg = new FleaseConfig(10000, 500, 500, new InetSocketAddress(12345), "localhost:12345",5);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        acceptor = new FleaseAcceptor(new LearnEventListener() {

            @Override
            public void learnedEvent(ASCIIString cellId, ASCIIString leaseHolder, long leaseTimeout_ms, long me) {
            }
        }, cfg, "/tmp/xtreemfs-test/", true);
        proposer = new FleaseProposer(cfg, acceptor, new FleaseCommunicationInterface() {

            @Override
            public void sendMessage(FleaseMessage msg, InetSocketAddress receiver) throws IOException {
            }

            @Override
            public void requestTimer(FleaseMessage msg, long timestamp) {
                if (msg.getMsgType() == FleaseMessage.MsgType.EVENT_RESTART) {
                    long wait = timestamp - TimeSync.getLocalSystemTime();
                    try {
                        Thread.sleep(wait);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(FleaseProposerTest.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    try {
                        proposer.processMessage(msg);
                    } catch (Exception ex) {
                        fail(ex.toString());
                    }
                }
            }
        }, new FleaseStatusListener() {

            @Override
            public void statusChanged(ASCIIString cellId, Flease lease) {
                // System.out.println("result: "+lease.getLeaseHolder());
                synchronized (result) {
                    result.set(new Flease(cellId, lease.getLeaseHolder(), lease.getLeaseTimeout_ms(),lease.getMasterEpochNumber()));
                    result.notify();
                }
            }

            @Override
            public void leaseFailed(ASCIIString cellId, FleaseException error) {
                // System.out.println("failed: "+error);
                FleaseProposerTest.fail(error.toString());
            }
        }, new LearnEventListener() {

            @Override
            public void learnedEvent(ASCIIString cellId, ASCIIString leaseHolder, long leaseTimeout_ms, long me) {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        },null,null);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testGetLease() throws Exception {

        proposer.openCell(TESTCELL, new ArrayList(),false);

        Thread.sleep(100);

        FleaseProposerCell cell = proposer.cells.get(TESTCELL);

        assertEquals(TESTCELL,cell.getMessageSent().getCellId());
        assertEquals(cfg.getIdentity(),cell.getMessageSent().getLeaseHolder());
        assertTrue(cell.getMessageSent().getLeaseTimeout() > TimeSync.getGlobalTime());


    }

    /*public void testHandoverLease() throws Exception {

        proposer.openCell(TESTCELL, new ArrayList(),false);


        Thread.sleep(100);

        FleaseProposerCell cell = proposer.cells.get(TESTCELL);

        assertEquals(TESTCELL,cell.getMessageSent().getCellId());
        assertEquals(cfg.getIdentity(),cell.getMessageSent().getLeaseHolder());
        assertTrue(cell.getMessageSent().getLeaseTimeout() > TimeSync.getGlobalTime());

        final ASCIIString HANDOVER = new ASCIIString("HANDOVER");

        proposer.handoverLease(TESTCELL, HANDOVER);

        Thread.sleep(100);

        cell = proposer.cells.get(TESTCELL);

        assertEquals(TESTCELL,cell.getMessageSent().getCellId());
        assertEquals(HANDOVER,cell.getMessageSent().getLeaseHolder());
        assertTrue(cell.getMessageSent().getLeaseTimeout() > TimeSync.getGlobalTime());


    }*/


//    public void testPriorProposal() throws Exception {
//
//        final ASCIIString otherId = new ASCIIString("YAGGAYAGGA");
//        final AtomicBoolean finished = new AtomicBoolean(false);
//
//        //initialize the acceptor
//        FleaseMessage tmp = new FleaseMessage(FleaseMessage.MsgType.MSG_ACCEPT);
//        tmp.setCellId(TESTCELL);
//        tmp.setProposalNo(new ProposalNumber(10, 123456));
//        tmp.setLeaseHolder(otherId);
//        tmp.setLeaseTimeout(TimeSync.getGlobalTime()-20000);
//        tmp.setSendTimestamp(TimeSync.getGlobalTime());
//        acceptor.processMessage(tmp);
//
//        tmp.setCellId(TESTCELL);
//        tmp.setProposalNo(new ProposalNumber(10, 123456));
//        tmp.setLeaseTimeout(TimeSync.getGlobalTime()+10000);
//        tmp.setSendTimestamp(TimeSync.getGlobalTime());
//        acceptor.processMessage(tmp);
//
//        proposer.openCell(TESTCELL, new ArrayList<InetSocketAddress>(),false);
//
//        System.out.println("start acquire lease...");
//        proposer.acquireLease(TESTCELL, new FleaseListener() {
//
//            public void proposalResult(ASCIIString cellId, ASCIIString leaseHolder, long leaseTimeout_ms) {
//                System.out.println("got lease!");
//                assertEquals(TESTCELL,cellId);
//                assertEquals(otherId,leaseHolder);
//                finished.set(true);
//            }
//
//            public void proposalFailed(ASCIIString cellId, Throwable cause) {
//                System.out.println("failed!");
//                fail(cause.toString());
//            }
//        });
//
//
//    }
//
//
//    public void testPriorProposal2() throws Exception {
//
//        final ASCIIString otherId = new ASCIIString("YAGGAYAGGA");
//        final AtomicBoolean finished = new AtomicBoolean(false);
//
//        //initialize the acceptor
//        FleaseMessage tmp = new FleaseMessage(FleaseMessage.MsgType.MSG_LEARN);
//        tmp.setCellId(TESTCELL);
//        tmp.setProposalNo(new ProposalNumber(10, 123456));
//        tmp.setLeaseHolder(otherId);
//        tmp.setLeaseTimeout(TimeSync.getGlobalTime()+10000);
//        tmp.setSendTimestamp(TimeSync.getGlobalTime());
//        acceptor.processMessage(tmp);
//
//        tmp.setCellId(TESTCELL);
//        tmp.setProposalNo(new ProposalNumber(10, 123456));
//        tmp.setLeaseTimeout(TimeSync.getGlobalTime()+10000);
//        tmp.setSendTimestamp(TimeSync.getGlobalTime());
//        acceptor.processMessage(tmp);
//
//        proposer.openCell(TESTCELL, new ArrayList<InetSocketAddress>(),false);
//
//        System.out.println("start acquire lease...");
//        proposer.acquireLease(TESTCELL, new FleaseListener() {
//
//            public void proposalResult(ASCIIString cellId, ASCIIString leaseHolder, long leaseTimeout_ms) {
//                System.out.println("got lease!");
//                assertEquals(TESTCELL,cellId);
//                assertEquals(otherId,leaseHolder);
//                finished.set(true);
//            }
//
//            public void proposalFailed(ASCIIString cellId, Throwable cause) {
//                System.out.println("failed!");
//                fail(cause.toString());
//            }
//        });
//
//
//    }
//
//    public void testValidLease() throws Exception {
//
//        final ASCIIString otherId = new ASCIIString("YAGGAYAGGA");
//        final AtomicBoolean finished = new AtomicBoolean(false);
//
//        //initialize the acceptor
//        FleaseMessage tmp = new FleaseMessage(FleaseMessage.MsgType.MSG_LEARN);
//        tmp.setCellId(TESTCELL);
//        tmp.setProposalNo(new ProposalNumber(10, 123456));
//        tmp.setLeaseHolder(otherId);
//        tmp.setLeaseTimeout(TimeSync.getGlobalTime()+10000);
//        tmp.setSendTimestamp(TimeSync.getGlobalTime());
//        acceptor.processMessage(tmp);
//
//        proposer.openCell(TESTCELL, new ArrayList<InetSocketAddress>(),false);
//
//        System.out.println("start acquire lease...");
//        proposer.acquireLease(TESTCELL, new FleaseListener() {
//
//            public void proposalResult(ASCIIString cellId, ASCIIString leaseHolder, long leaseTimeout_ms) {
//                System.out.println("got lease!");
//                assertEquals(TESTCELL,cellId);
//                assertEquals(otherId,leaseHolder);
//                finished.set(true);
//            }
//
//            public void proposalFailed(ASCIIString cellId, Throwable cause) {
//                System.out.println("failed!");
//                fail(cause.toString());
//            }
//        });
//    }
//
//    public void testLeaseInGP() throws Exception {
//
//        System.out.println("TEST testLeaseInGP");
//
//        final ASCIIString otherId = new ASCIIString("YAGGAYAGGA");
//        final AtomicBoolean finished = new AtomicBoolean(false);
//
//        //initialize the acceptor
//        FleaseMessage tmp = new FleaseMessage(FleaseMessage.MsgType.MSG_LEARN);
//        tmp.setCellId(TESTCELL);
//        tmp.setProposalNo(new ProposalNumber(10, 123456));
//        tmp.setLeaseHolder(otherId);
//        tmp.setLeaseTimeout(TimeSync.getGlobalTime()+100);
//        tmp.setSendTimestamp(TimeSync.getGlobalTime());
//        acceptor.processMessage(tmp);
//
//        proposer.openCell(TESTCELL, new ArrayList<InetSocketAddress>(),false);
//
//        System.out.println("start acquire lease...");
//        proposer.acquireLease(TESTCELL, new FleaseListener() {
//
//            public void proposalResult(ASCIIString cellId, ASCIIString leaseHolder, long leaseTimeout_ms) {
//                System.out.println("got lease!");
//                assertEquals(TESTCELL,cellId);
//                assertEquals(cfg.getIdentity(),leaseHolder);
//                finished.set(true);
//            }
//
//            public void proposalFailed(ASCIIString cellId, Throwable cause) {
//                System.out.println("failed!");
//                fail(cause.toString());
//            }
//        });
//
//
//    }





}
