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
package org.xtreemfs.foundation.flease.proposer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xtreemfs.foundation.TimeSync;
import org.xtreemfs.foundation.buffer.ASCIIString;
import org.xtreemfs.foundation.flease.Flease;
import org.xtreemfs.foundation.flease.FleaseConfig;
import org.xtreemfs.foundation.flease.FleaseStage;
import org.xtreemfs.foundation.flease.FleaseStatusListener;
import org.xtreemfs.foundation.flease.FleaseViewChangeListenerInterface;
import org.xtreemfs.foundation.flease.MasterEpochHandlerInterface;
import org.xtreemfs.foundation.flease.acceptor.FleaseAcceptor;
import org.xtreemfs.foundation.flease.acceptor.LearnEventListener;
import org.xtreemfs.foundation.flease.comm.FleaseCommunicationInterface;
import org.xtreemfs.foundation.flease.comm.FleaseMessage;
import org.xtreemfs.foundation.flease.comm.FleaseMessage.MsgType;
import org.xtreemfs.foundation.flease.comm.ProposalNumber;
import org.xtreemfs.foundation.flease.proposer.CellAction.ActionName;
import org.xtreemfs.foundation.flease.proposer.FleaseProposerCell.State;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author bjko
 */
public class FleaseProposer {
    private static final Logger LOG = LoggerFactory.getLogger(FleaseProposer.class);

    final Map<ASCIIString, FleaseProposerCell> cells;

    final FleaseConfig config;

    final FleaseAcceptor localAcceptor;

    final FleaseCommunicationInterface comm;

    //private long lastBallotNo;

    private FleaseViewChangeListenerInterface viewListener;

    private final FleaseStatusListener          leaseListener;

    private final LearnEventListener            evListener;

    private final FleaseLocalQueueInterface     localQueue;

    private final MasterEpochHandlerInterface   meHandler;

    public FleaseProposer(
            FleaseConfig config,
            FleaseAcceptor localAcceptor,
            FleaseCommunicationInterface comm,
            FleaseStatusListener leaseListener,
            LearnEventListener evListener,
            FleaseLocalQueueInterface localQueue,
            MasterEpochHandlerInterface meHandler) {
        cells = new HashMap<ASCIIString, FleaseProposerCell>(100000);
        this.config = config;
        this.localAcceptor = localAcceptor;
        this.comm = comm;
        assert(leaseListener != null);
        this.leaseListener = leaseListener;
        this.evListener = evListener;
        this.localQueue = localQueue;
        this.meHandler = meHandler;
        assert ((meHandler == null) || (meHandler != null) && (localQueue != null));
    }

    public void setViewChangeListener(FleaseViewChangeListenerInterface listener) {
        viewListener = listener;
    }

    public void setViewId(ASCIIString cellId, int viewId) throws FleaseException {
        FleaseProposerCell cell = cells.get(cellId);
        if (cell == null) {
            throw new FleaseException("cell must be opened before any operation!");
        }
        cell.setViewId(viewId);
        cell.addAction(ActionName.PROPOSER_SET_VIEWID, Integer.toString(viewId));
    }

    public Flease updatePrevLeaseForCell(ASCIIString cellId, Flease lease) {
        FleaseProposerCell cell = cells.get(cellId);
        if (cell == null) {
            return null;
        }
        final Flease prevLease = cell.getPrevLease();
        if (!prevLease.equals(lease)) {
            cell.setPrevLease(lease);
            return prevLease;
        }
        return null;
    }

    public ProposalNumber getCurrentBallotNo(ASCIIString cellId) {
        FleaseProposerCell cell = cells.get(cellId);
        if (cell == null) {
            return ProposalNumber.EMPTY_PROPOSAL_NUMBER;
        }
        return cell.getBallotNo();
    }

    public void openCell(
            ASCIIString cellId,
            List<InetSocketAddress> acceptors,
            boolean requestMasterEpoch) throws FleaseException {
        FleaseProposerCell cell = cells.get(cellId);
        if (cell == null) {
            cell = new FleaseProposerCell(cellId, acceptors, config.getSenderId());
            cell.setCellState(State.IDLE);
            cell.setRequestMasterEpoch(requestMasterEpoch);
            cells.put(cellId, cell);
            cell.addAction(ActionName.PROPOSER_CELL_OPENED);
            LOG.warn("P created new cellId {}",
                cellId);
            acquireLease(cell);
        } else {
            throw new FleaseException("cell already opened");
        }
    }

    public void closeCell(ASCIIString cellId) {
        FleaseProposerCell cell = cells.remove(cellId);
        if (cell != null) {
            cell.addAction(ActionName.PROPOSER_CELL_CLOSED);
        }
    }

    private void acquireLease(FleaseProposerCell cell) throws FleaseException {
        cell.addAction(ActionName.PROPOSER_ACQUIRE_LEASE);
        final ASCIIString cellId = cell.getCellId();
        //check local results
        FleaseMessage localInfo = localAcceptor.getLocalLeaseInformation(cellId);
        if ((localInfo != null) && (localInfo.hasNotTimedOut(config, TimeSync.getGlobalTime()))) {
            //we can safely return the learned values
            LOG.debug("P request served from local state: {}",
                cellId);
            cell.addAction(ActionName.PROPOSER_RETURNED_LOCAL_LEASE);
            evListener.learnedEvent(localInfo.getCellId(),
                    localInfo.getLeaseHolder(),
                    localInfo.getLeaseTimeout(),
                    localInfo.getMasterEpochNumber());
            return;
        }

        if (cell.getCellState() == State.IDLE) {
            //can safely start the proposal
            cell.setNumFailures(0);
            cell.setHandoverTo(null);
            startPrepare(cell,config.getIdentity());
        } else {
            cell.addAction(ActionName.PROPOSER_ACQUIRE_LEASE, "not idle");
            LOG.warn("P cellId {} is not idle, ignoring acquireLease",
                cellId);
        }
    }

    public void renewLease(ASCIIString cellId) throws FleaseException {
        FleaseProposerCell cell = cells.get(cellId);
        if (cell == null) {
            LOG.debug("P ignore renew for closed/unknown cell {}", cellId);
            return;
        }
        cell.addAction(ActionName.PROPOSER_RENEW_LEASE);
        //fixme

        if (cell.isHandoverInProgress()) {
            LOG.info("handover in progress for cell {}, renew canceled",
                    cell.getCellId());
            return;
        }

        //check local state
        FleaseMessage localInfo = localAcceptor.getLocalLeaseInformation(cellId);

        if (localInfo == null) {
            cell.addAction(ActionName.PROPOSER_RENEW_FAILED_NO_LOCAL_LEASE_INFO);
            throw new FleaseException("cannot renew lease, no local lease information!");
        }
        if (!localInfo.getLeaseHolder().equals(config.getIdentity())) {
            cell.addAction(ActionName.PROPOSER_RENEW_FAILED_NOT_OWNER);
            throw new FleaseException("cannot renew lease, not lease owner (owner is "
                    + localInfo.getLeaseHolder() + ")!");
        }
        if (localInfo.hasTimedOut(config, TimeSync.getGlobalTime())) {
            cell.addAction(ActionName.PROPOSER_RENEW_FAILED_LEASE_TO);
            throw new FleaseException("cannot renew lease, lease already timed out! "
                    + (localInfo.getLeaseTimeout() - config.getDMax())
                    + " < " + TimeSync.getGlobalTime());
        }
        //make sure there is enough time before timeout
        if (localInfo.getLeaseTimeout() - config.getDMax()
                < TimeSync.getGlobalTime() + config.getRoundTimeout() * 2) {
            cell.addAction(ActionName.PROPOSER_RENEW_FAILED_LEASE_NOT_ENOUGH_TIME);
            throw new FleaseException("cannot renew lease, not enough time left for renew! "
                    + (localInfo.getLeaseTimeout() - config.getDMax())
                    + " < "
                    + (TimeSync.getGlobalTime() + config.getRoundTimeout() * 2));
        }

        //no need for a new master epoch upon renew!
        cell.setRequestMasterEpoch(false);

        //we can safely set the instance no +1
        //ONLY ALLOWED DURING RENEW!

        startPrepare(cell,config.getIdentity());
    }

    public void handoverLease(ASCIIString cellId, ASCIIString newOwner) throws FleaseException {
        // TODO(bjko): Change to return lease.
        FleaseProposerCell cell = cells.get(cellId);
        if (cell == null) {
            LOG.debug("P ignore renew for closed/unknown cell {}",
                cellId);
            return;
        }
        cell.addAction(ActionName.PROPOSER_HANDOVER_LEASE);
        //fixme

        //check local state
        FleaseMessage localInfo = localAcceptor.getLocalLeaseInformation(cellId);

        if (localInfo == null) {
            throw new FleaseException("cannot handover lease, no local lease information!");
        }
        if (!localInfo.getLeaseHolder().equals(config.getIdentity())) {
            throw new FleaseException("cannot handover lease, not lease owner (owner is "
                    + localInfo.getLeaseHolder() + ")!");
        }
        //make sure there is enough time before timeout
        if (localInfo.getLeaseTimeout() - config.getDMax()
                < TimeSync.getGlobalTime() + config.getRoundTimeout() * 2) {
            throw new FleaseException("cannot handover lease, not enough time left for renew! "
                + (localInfo.getLeaseTimeout() - config.getDMax())
                + " < "
                + (TimeSync.getGlobalTime() + config.getRoundTimeout() * 2));
        }
        //we can safely set the instance no +1
        //ONLY ALLOWED DURING RENEW!

        cell.setHandoverTo(newOwner);
        if (cell.getCellState() == State.IDLE) {
            //set state to unknown
            evListener.learnedEvent(localInfo.getCellId(),
                    null,
                    0,
                    FleaseMessage.IGNORE_MASTER_EPOCH);
            startPrepare(cell,newOwner);
        }
    }

    public void processMessage(FleaseMessage msg) throws Exception {
        //get the cell
        FleaseProposerCell cell = cells.get(msg.getCellId());
        if (cell == null) {
            LOG.debug("P drop message for unknown cellId {} from {}",
                        msg.getCellId(),
                        msg.getSender());
            return;
        }

        try {
            //reject anything if viewID < own viewID or if own viewId is -1
            //ignore if viewId > own viewID but notify listener
            final int myViewId = cell.getViewId();
            if (myViewId == FleaseMessage.VIEW_ID_INVALIDATED) {
                //just ignore things
                LOG.debug("P drop message because of INVALIDATED local view for cell {}",
                    cell.getCellId());
                return;
            }

            if (myViewId > msg.getViewId()) {
                //drop
                LOG.debug("P drop message because of outdated remote view for cell {}: local view {}, remote {}",
                    cell.getCellId(),
                    myViewId,
                    msg.getViewId());
                return;
            }

            //events
            switch (cell.getCellState()) {
                case IDLE: {
                    if (msg.getMsgType() == FleaseMessage.MsgType.EVENT_RESTART) {
                        cell.addAction(ActionName.PROPOSER_RESTART_EVENT);
                        LOG.debug("P restart event: {}", msg);
                        startPrepare(cell,config.getIdentity());
                    } else if (msg.getMsgType() == FleaseMessage.MsgType.EVENT_RENEW) {
                        cell.addAction(ActionName.PROPOSER_RENEW_EVENT);
                        LOG.debug("P renew event: {}", msg);
                        try {
                            renewLease(cell.getCellId());
                        } catch (FleaseException ex) {
                            cell.addAction(ActionName.PROPOSER_INTERNAL_ERROR_CELL_RESET,
                                    ex.toString());
                            LOG.error("P renew failed for cell {}: {}",
                                    cell.getCellId(),
                                    ex);
                            // Reset cell.
                            cell.setCellState(State.IDLE);
                            cell.setMessageSent(null);
                            cell.setBallotNo(new ProposalNumber(cell.getBallotNo().getProposalNo() + 1,
                                    cell.getBallotNo().getSenderId()));
                            cell.getResponses().clear();
                            cell.setNumFailures(0);
                            // Schedule restart.
                            final int wait_ms = (int) config.getDMax() + config.getMaxLeaseTimeout();
                            LOG.debug("P cannot renew cell {}, scheduled restart in {} ms",
                                    cell.getCellId(), wait_ms);
                            cell.addAction(ActionName.PROPOSER_SCHEDULED_RESTART);
                            FleaseMessage timer = new FleaseMessage(MsgType.EVENT_RESTART);
                            timer.setCellId(cell.getCellId());
                            timer.setProposalNo(cell.getBallotNo());
                            comm.requestTimer(timer, TimeSync.getLocalSystemTime() + wait_ms);
                        }
                    } else {
                        //ignore everything else
                        LOG.debug("P droped message in state IDLE: {}", msg);
                    }
                    break;
                }
                case WAIT_FOR_PREP_ACK: {
                    processPrepareResponse(cell, msg);
                    break;
                }
                case WAIT_FOR_ACCEPT_ACK: {
                    processAcceptResponse(cell, msg);
                    break;
                }
            }
        } catch (Throwable throwable) {
            LOG.error("Exception in proposer: {}, cell {}", throwable, cell);
            throw new Exception(throwable);
        }
    }

    protected void startPrepare(FleaseProposerCell cell, ASCIIString leaseHolder) {
        cell.addAction(ActionName.PROPOSER_PREPARE_START);
        assertState(cell.getCellState() == State.IDLE, cell);

        if (cell.getLastPrepareTimestamp_ms() + config.getMaxLeaseTimeout()
                < TimeSync.getLocalSystemTime()) {
            //timed out, set new ballot number
            LOG.debug("P reset cell id {} due to timeout",
                        cell.getCellId());
            cell.setBallotNo(new ProposalNumber(TimeSync.getGlobalTime(), config.getSenderId()));
            cell.addAction(ActionName.PROPOSER_SET_BALLOT_NO, cell.getBallotNo().toString());
        }

        //propose myself
        cell.getResponses().clear();
        cell.touch();

        //assert (lastBallotNo < cell.getBallotNo().getProposalNo()) : ("lastBallotNo="+lastBallotNo+", cell="+cell.getBallotNo().getProposalNo());
        assertState(cell.getBallotNo() != null, cell);
        assertState(cell.getBallotNo().getSenderId() == config.getSenderId(), cell);

        //lastBallotNo = cell.getBallotNo().getProposalNo();

        final int numRemoteAcc = cell.getNumAcceptors();
        FleaseMessage msg = new FleaseMessage(FleaseMessage.MsgType.MSG_PREPARE);
        msg.setCellId(cell.getCellId());
        msg.setProposalNo(cell.getBallotNo());
        msg.setLeaseHolder(leaseHolder);
        msg.setLeaseTimeout(TimeSync.getGlobalTime() + config.getMaxLeaseTimeout());
        msg.setSendTimestamp(TimeSync.getGlobalTime());
        msg.setSender(null);
        if (cell.isRequestMasterEpoch()) {
            msg.setMasterEpochNumber(FleaseMessage.REQUEST_MASTER_EPOCH);
            cell.addAction(ActionName.PROPOSER_REQUEST_MASTER_EPOCH);
        }
        cell.setMessageSent(msg);

        LOG.debug("P start PREPARE: {}", msg);


        for (int i = 0; i < numRemoteAcc; i++) {
            try {
                LOG.debug("P send prepare to: {}", cell.getAcceptors().get(i));
                Thread.yield();
                comm.sendMessage(msg, cell.getAcceptors().get(i));
            } catch (IOException ex) {
                //cancel(cell, ex, 0);
            }
        }

        FleaseMessage timer = new FleaseMessage(MsgType.EVENT_TIMEOUT_PREPARE);
        timer.setCellId(cell.getCellId());
        timer.setProposalNo(cell.getBallotNo());
        timer.validateMessage();
        comm.requestTimer(timer, TimeSync.getLocalSystemTime() + config.getRoundTimeout());
        cell.addAction(ActionName.PROPOSER_SCHEDULED_TIMEOUT);

        assertState(cell.getMessageSent() != null, cell);
        assertState(cell.getMessageSent().getProposalNo() != null, cell);
        cell.setCellState(State.WAIT_FOR_PREP_ACK);

        final FleaseMessage localResponse = localAcceptor.handlePREPARE(msg);
        if (localResponse != null) {
            if (meHandler != null
                && cell.isRequestMasterEpoch()
                && localResponse.getMsgType() == FleaseMessage.MsgType.MSG_PREPARE_ACK) {
                meHandler.sendMasterEpoch(localResponse,
                        new MasterEpochHandlerInterface.Continuation() {
                            @Override
                            public void processingFinished() {
                                localQueue.enqueueMessage(localResponse);
                            }
                        });
            } else {
                processPrepareResponse(cell, localResponse);
            }
        }

    }

    protected void processPrepareResponse(FleaseProposerCell cell, FleaseMessage msg) {
        cell.addAction(ActionName.PROPOSER_PREPARE_PROCESS_RESPONSE);
        assertState(cell.getCellState() == State.WAIT_FOR_PREP_ACK, cell);
        assertState(cell.getMessageSent() != null, cell);

        if (msg.getMsgType() != MsgType.MSG_PREPARE_ACK
                && msg.getMsgType() != MsgType.MSG_PREPARE_NACK
                && msg.getMsgType() != MsgType.MSG_WRONG_VIEW
                && msg.getMsgType() != MsgType.EVENT_TIMEOUT_PREPARE) {
            LOG.debug("P ignore message (UNEXPECTED MESSAGE TYPE {}): {}",
                        msg.getMsgType(),
                        msg);
            return;
        }

        if (msg.getSendTimestamp() + config.getMessageTimeout() < TimeSync.getGlobalTime()) {
            //drop outdated message
            LOG.debug("P ignore message (too old): {}", msg);
            return;
        }

        final long currentTime = TimeSync.getGlobalTime();
        if (msg.getSendTimestamp() > currentTime + config.getDMax()) {
            cell.addAction(ActionName.PROPOSER_RECEIVED_OUT_OF_SYNC_MSG,
                    msg.getSendTimestamp()
                    + " > "
                    + currentTime
                    + "+"
                    + config.getDMax());
            //drop outdated message
            LOG.warn("RECEIVED MESSAGE WITH TIMESTAMP TOO FAR IN THE FUTURE (likely cause: "
                            + "clocks aren't in sync). SYSTEM IS NOT IN A SAFE STATE. Msg TS {} > current Time {} + dMax {}. FleaseMessage Details: {}",
                    msg.getSendTimestamp(),
                    currentTime,
                    config.getDMax(),
                    msg.toString());
            cell.addAction(ActionName.PROPOSER_PREPARE_FAILED);
            cancel(cell,
                   new FleaseException("System is not in sync (clock sync drift exceeded)!"), 0);
            return;
        }

        if (msg.before(cell.getMessageSent())) {
            LOG.debug("P ignore message (before my request): {}", msg);
            return;
        }

        if (msg.getMsgType() == MsgType.EVENT_TIMEOUT_PREPARE) {
            cell.addAction(ActionName.PROPOSER_PREPARE_TIMEOUT);
            //not enough responses :( abort or re-try
            cancel(cell, new FleaseException("did not receive enough responses for PREPARE"), 0);
            return;
        }

        final List<FleaseMessage> responses = cell.getResponses();
        responses.add(msg);
        if (cell.majorityAvail()) {
            LOG.debug("P majority responded for proposal {}: {}",
                        cell.getBallotNo(),
                        responses.size());
            //analyze responses
            ProposalNumber maxBallot = ProposalNumber.EMPTY_PROPOSAL_NUMBER;
            FleaseMessage prevAccepted = new FleaseMessage(MsgType.MSG_ACCEPT);
            int maxViewId = 0;
            for (FleaseMessage resp : responses) {
                switch (resp.getMsgType()) {

                    case MSG_WRONG_VIEW: {
                        if (resp.getViewId() > maxViewId) {
                            maxViewId = resp.getViewId();
                        }
                        break;
                    }

                    case MSG_PREPARE_ACK: {
                        //check for previously accepted proposals
                        if (!resp.getPrevProposalNo().isEmpty()) {
                            if (prevAccepted == null
                                || resp.getPrevProposalNo().after(
                                        prevAccepted.getPrevProposalNo())) {
                                prevAccepted = resp;
                            }
                        }
                        break;
                    }

                    case MSG_PREPARE_NACK: {
                        if (resp.getPrevProposalNo().after(maxBallot)) {
                            maxBallot = resp.getPrevProposalNo();
                        }
                        break;
                    }

                }
            }

            if (maxViewId != cell.getViewId()) {
                cell.addAction(ActionName.PROPOSER_VIEW_OUTDATED,
                        maxViewId + "!=" + cell.getViewId());
                LOG.debug("P prepare failed due to outdated view local={} max={}",
                            cell.getViewId(),
                            maxViewId);
                viewListener.viewIdChangeEvent(cell.getCellId(), maxViewId);
                cell.addAction(ActionName.PROPOSER_PREPARE_FAILED);
                cancel(cell, new FleaseException("local viewId is outdated"), 0);
                return;
            }

            if (!maxBallot.isEmpty()) {
                cell.addAction(ActionName.PROPOSER_PREPARE_OVERRULED);
                cell.setBallotNo(new ProposalNumber(
                        maxBallot.getProposalNo() + (int) (Math.random() * 10) + 1,
                        cell.getBallotNo().getSenderId()));
                LOG.debug("P prepare OVERRULED by {}, restart with ballot number: {}",
                            maxBallot.getProposalNo(),
                            cell.getBallotNo());
                //request a timer event for restart
                cancel(cell,
                        new FleaseException("local proposal was overruled by remote proposal"), 0);
                return;
            }

            if (!prevAccepted.getProposalNo().isEmpty()) {
                //change our values for accept

                //check prev accepted value
                if (prevAccepted.hasNotTimedOut(config, TimeSync.getGlobalTime())) {
                    //we must use the previous value
                    //except for renew instances
                    if (prevAccepted.getLeaseHolder().equals(config.getIdentity())) {
                        //renew!
                        assertState(
                                cell.getMessageSent().getLeaseHolder().equals(config.getIdentity())
                                || cell.isHandoverInProgress(),
                                cell);
                        assertState(
                                cell.getMessageSent().getLeaseTimeout() >=
                                    prevAccepted.getLeaseTimeout(),
                                cell);
                        cell.addAction(ActionName.PROPOSER_PREPARE_IMPLICIT_RENEW);
                            LOG.debug("P prepare ACK processing with my proposal (renew): "
                                        + "prev={}/{} {}",
                                    prevAccepted.getLeaseHolder(),
                                    prevAccepted.getLeaseTimeout(),
                                    prevAccepted.getPrevProposalNo());
                    } else {
                        //use other value
                        cell.addAction(ActionName.PROPOSER_PREPARE_PRIOR_VALUE);
                        LOG.debug("P prepare ACK processing with prior proposal (still valid): prev={}/{} {}",
                                    prevAccepted.getLeaseHolder(),
                                    prevAccepted.getLeaseTimeout(),
                                    prevAccepted.getPrevProposalNo());
                        cell.getMessageSent().setLeaseHolder(prevAccepted.getLeaseHolder());
                        cell.getMessageSent().setLeaseTimeout(prevAccepted.getLeaseTimeout());
                    }
                } else {
                    if (prevAccepted.hasTimedOut(config, TimeSync.getGlobalTime())) {
                        cell.addAction(ActionName.PROPOSER_PREPARE_LEASE_TO);
                        LOG.debug("P prepare ACK processing with my proposal (old lease has timed out): prev={}/{} {}",
                                    prevAccepted.getLeaseHolder(),
                                    prevAccepted.getLeaseTimeout(),
                                    prevAccepted.getPrevProposalNo());
                    } else {
                        //unknown state of the lease, must use prev value
                        cell.addAction(ActionName.PROPOSER_PREPARE_PRIOR_VALUE);
                        LOG.debug("P processing with prior proposal (old lease is in GP): {}/{} {}",
                                    prevAccepted.getLeaseHolder(),
                                    prevAccepted.getLeaseTimeout(),
                                    prevAccepted.getPrevProposalNo());
                        cell.getMessageSent().setLeaseHolder(prevAccepted.getLeaseHolder());
                        cell.getMessageSent().setLeaseTimeout(prevAccepted.getLeaseTimeout());
                    }
                }
            } else {
                cell.addAction(ActionName.PROPOSER_PREPARE_EMPTY);
                LOG.debug("P processing with no prior proposal");
            }

            //master epoch
            if (cell.isRequestMasterEpoch()) {
                long maxMasterEpoch = -1;
                for (FleaseMessage resp : responses) {
                    if (resp.getMsgType() == FleaseMessage.MsgType.MSG_PREPARE_ACK) {
                        if (resp.getMasterEpochNumber() > maxMasterEpoch) {
                            maxMasterEpoch = resp.getMasterEpochNumber();
                        }
                    }
                }
                assert(maxMasterEpoch > -1) : "no valid master epoch sent: "+maxMasterEpoch;
                cell.setMasterEpochNumber(maxMasterEpoch+1);
                cell.addAction(ActionName.PROPOSER_PREPARE_MAX_MASTER_EPOCH,
                        Long.toString(maxMasterEpoch));
                LOG.debug("P using masterEpoch {}", maxMasterEpoch+1);
            }

            responses.clear();

            cell.addAction(ActionName.PROPOSER_PREPARE_SUCCESS);
            startAccept(cell);
        }
    }

    public void startAccept(FleaseProposerCell cell) {
        cell.addAction(ActionName.PROPOSER_ACCEPT_START);
        cell.setCellState(State.WAIT_FOR_ACCEPT_ACK);

        final int numRemoteAcc = cell.getNumAcceptors();

        FleaseMessage msg = new FleaseMessage(FleaseMessage.MsgType.MSG_ACCEPT);
        msg.setCellId(cell.getCellId());
        msg.setProposalNo(cell.getBallotNo());
        msg.setLeaseHolder(cell.getMessageSent().getLeaseHolder());
        msg.setLeaseTimeout(cell.getMessageSent().getLeaseTimeout());
        msg.setSendTimestamp(TimeSync.getGlobalTime());
        msg.setSender(null);
        if (cell.isRequestMasterEpoch())
            msg.setMasterEpochNumber(cell.getMasterEpochNumber());
        cell.setMessageSent(msg);

        LOG.debug("P start ACCEPT: {}", msg);

        for (int i = 0; i < numRemoteAcc; i++) {
            try {
                LOG.debug("P send accept to: {}", cell.getAcceptors().get(i));
                Thread.yield();
                comm.sendMessage(msg, cell.getAcceptors().get(i));
            } catch (IOException ex) {
                //cancel(cell, ex, 0);
            }
        }

        cell.addAction(ActionName.PROPOSER_SCHEDULED_TIMEOUT);
        FleaseMessage timer = new FleaseMessage(MsgType.EVENT_TIMEOUT_ACCEPT);
        timer.setCellId(cell.getCellId());
        timer.setProposalNo(cell.getBallotNo());
        timer.validateMessage();
        comm.requestTimer(timer, TimeSync.getLocalSystemTime() + config.getRoundTimeout());

        final FleaseMessage localResponse = localAcceptor.handleACCEPT(msg);
        if (localResponse != null) {
            if (meHandler != null
                && cell.isRequestMasterEpoch()
                && localResponse.getMsgType() == FleaseMessage.MsgType.MSG_ACCEPT_ACK) {
                meHandler.storeMasterEpoch(localResponse,
                        new MasterEpochHandlerInterface.Continuation() {
                            @Override
                            public void processingFinished() {
                                localQueue.enqueueMessage(localResponse);
                            }
                        });
            } else {
                processAcceptResponse(cell, localResponse);
            }
        }

    }

    protected void processAcceptResponse(FleaseProposerCell cell, FleaseMessage msg) {
        cell.addAction(ActionName.PROPOSER_ACCEPT_PROCESS_RESPONSE);
        assertState(cell.getCellState() == State.WAIT_FOR_ACCEPT_ACK, cell);
        assertState(cell.getMessageSent() != null, cell);

        if (msg.getSendTimestamp() + config.getMessageTimeout() < TimeSync.getGlobalTime()) {
            //drop outdated message
            LOG.debug("P ignore message (too old): {}", msg);
            return;
        }

        if (msg.getSendTimestamp() > TimeSync.getGlobalTime() + config.getDMax()) {
            //drop outdated message
            cell.addAction(ActionName.PROPOSER_RECEIVED_OUT_OF_SYNC_MSG,
                    msg.getSendTimestamp()
                    + " > "
                    + TimeSync.getGlobalTime()
                    + "+"
                    + config.getDMax());
            LOG.warn("RECEIVED MESSAGE WITH TIMESTAMP TOO FAR IN THE FUTURE "
                            + "(likely cause: clocks aren't in sync). "
                            + "SYSTEM IS NOT IN A SAFE STATE: {}",
                    msg);
            cell.addAction(ActionName.PROPOSER_ACCEPT_FAILED);
            cancel(cell,
                    new FleaseException("System is not in sync (clock sync drift exceeded)!"), 0);
            return;
        }

        if (msg.getMsgType() != MsgType.MSG_ACCEPT_ACK
                && msg.getMsgType() != MsgType.MSG_ACCEPT_NACK
                && msg.getMsgType() != MsgType.MSG_WRONG_VIEW
                && msg.getMsgType() != MsgType.EVENT_TIMEOUT_ACCEPT) {
            LOG.debug("P ignore message (unexpected message type): {}", msg);
            return;
        }

        if (msg.before(cell.getMessageSent())) {
                LOG.debug("P ignore message (before my request): {}", msg);
            return;
        }

        if (msg.getMsgType() == MsgType.EVENT_TIMEOUT_ACCEPT) {
            //not enough responses :( abort or re-try
            cell.addAction(ActionName.PROPOSER_ACCEPT_TIMEOUT);
            cancel(cell, new FleaseException("did not receive enough responses for ACCEPT"), 0);
            return;
        }

        final List<FleaseMessage> responses = cell.getResponses();
        responses.add(msg);
        if (cell.majorityAvail()) {
            LOG.debug("P majority responded for proposal {}: {}",
                        cell.getBallotNo(), responses.size());
            //analyze responses
            int maxViewId = 0;
            ProposalNumber maxBallot = ProposalNumber.EMPTY_PROPOSAL_NUMBER;
            for (FleaseMessage resp : responses) {
                if (resp.getViewId() > maxViewId) {
                    maxViewId = resp.getViewId();
                } else if (resp.getMsgType() == MsgType.MSG_ACCEPT_NACK) {
                    maxBallot = resp.getPrevProposalNo();
                }
            }

            if (maxViewId != cell.getViewId()) {
                LOG.debug("P accept failed due to outdated view local={} max={}",
                    cell.getViewId(),
                    maxViewId);

                viewListener.viewIdChangeEvent(cell.getCellId(), maxViewId);
                cancel(cell, new FleaseException("local viewId is outdated"), 0);
                return;
            }

            //take action
            if (!maxBallot.isEmpty()) {
                cell.addAction(ActionName.PROPOSER_ACCEPT_OVERRULED);
                cell.setBallotNo(new ProposalNumber(
                        maxBallot.getProposalNo() + (int) (Math.random() * 10) + 1,
                        cell.getBallotNo().getSenderId()));
                LOG.debug("P accept  OVERRULED by {}, restart with ballot number: {}",
                            maxBallot.getProposalNo(), cell.getBallotNo());
                //request a timer event for restart
                cancel(cell,
                        new FleaseException("proposal was overruled by remote proposal "
                                +"during ACCEPT"),
                        0);
                return;
            }

            responses.clear();

            cell.addAction(ActionName.PROPOSER_ACCEPT_SUCCESS);
            learn(cell);
        }
    }

    public void learn(FleaseProposerCell cell) {
        cell.addAction(ActionName.PROPOSER_LEARN_START);
        cell.setCellState(State.IDLE);

        final int numRemoteAcc = cell.getNumAcceptors();
        FleaseMessage msg = new FleaseMessage(FleaseMessage.MsgType.MSG_LEARN);
        msg.setCellId(cell.getCellId());
        msg.setProposalNo(cell.getBallotNo());
        msg.setLeaseHolder(cell.getMessageSent().getLeaseHolder());
        msg.setLeaseTimeout(cell.getMessageSent().getLeaseTimeout());
        msg.setSendTimestamp(TimeSync.getGlobalTime());
        msg.setSender(null);
        if (cell.isRequestMasterEpoch()) {
            assertState(cell.getMasterEpochNumber() > -1, cell);
            msg.setMasterEpochNumber(cell.getMasterEpochNumber());
        }
        cell.setMessageSent(msg);


        //check the result...
        //if the lease is already outdated, we can immediately retry
        //if thg lease is in the grace period, we have to trigger a timer

        if (msg.hasTimedOut(config, TimeSync.getGlobalTime())) {
            //timed out
            //retry
            cell.addAction(ActionName.PROPOSER_LEARN_TIMED_OUT);
            cell.setBallotNo(new ProposalNumber(
                    cell.getBallotNo().getProposalNo() + 1,
                    cell.getBallotNo().getSenderId()));

            startPrepare(cell,cell.getMessageSent().getLeaseHolder());

            LOG.debug("P finished round, lease has timed out, restart prepare: {}",
                        msg.toString());

        } else if (msg.hasNotTimedOut(config, TimeSync.getGlobalTime())) {
            //lease is valid, do learn step

            cell.setBallotNo(new ProposalNumber(
                    cell.getBallotNo().getProposalNo() + 1,
                    cell.getBallotNo().getSenderId()));

            LOG.debug("P finished round, lease is valid: {}", msg);


            //only here does a learn make sense
            if (config.isSendLearnMessages()) {
                LOG.debug("P start LEARN: {}", msg);

                for (int i = 0; i < numRemoteAcc; i++) {
                    try {
                        comm.sendMessage(msg, cell.getAcceptors().get(i));
                    } catch (IOException ex) {
                        //ignore them here
                    }
                }
            }

            //FIXME: local message
            //msgs[numRemoteAcc] = new FleaseMessage(msg);
            //msgs[numRemoteAcc].setSender(null);
            localAcceptor.handleLEARN(msg);

            if (!FleaseStage.DISABLE_RENEW_FOR_TESTING
                && cell.getMessageSent().getLeaseHolder().equals(config.getIdentity())) {
                //renew after half of the time
                //FIXE: could be relaxed
                final long renewTime =
                        cell.getMessageSent().getLeaseTimeout() - config.getRoundTimeout() * 4;
                if (TimeSync.getGlobalTime() < renewTime) {
                    cell.addAction(ActionName.PROPOSER_SCHEDULED_RENEW);
                    final FleaseMessage timer = new FleaseMessage(MsgType.EVENT_RENEW);
                    timer.setCellId(cell.getCellId());
                    timer.setProposalNo(cell.getBallotNo());

                    comm.requestTimer(timer, globalToLocalTime(renewTime));
                    LOG.debug("scheduled renew for {} at {}",
                                cell.getCellId(),
                                globalToLocalTime(renewTime));
                } else {
                    cell.addAction(ActionName.PROPOSER_LEARN_TIMED_OUT);
                    cell.addAction(ActionName.PROPOSER_SCHEDULED_RESTART);
                    final int wait_ms = (int)
                            (msg.getLeaseTimeout() - TimeSync.getGlobalTime() + config.getDMax());
                    LOG.warn("too late to schedule renew for cell {}, restart in {} ms (now={}, renew={})",
                            cell.getCellId(), wait_ms, TimeSync.getGlobalTime(), renewTime);
                    //schedule a retry instead
                    cancel(cell,
                            new FleaseException("too late for renew, re-start after lease "
                                    + "has timed out in "
                                    + wait_ms
                                    + "ms"),
                            wait_ms);
                }
            }

        } else {
            //grace period
            cell.addAction(ActionName.PROPOSER_LEARN_LEASE_IN_GRACE_PERIOD);
            cell.addAction(ActionName.PROPOSER_SCHEDULED_RESTART);
            final int wait_ms = (int)
                    (msg.getLeaseTimeout() - TimeSync.getGlobalTime() + config.getDMax());
            LOG.debug("P finished round, lease is in grace period, scheduled restart in {} ms: {}",
                        wait_ms, msg);
            cancel(cell, new FleaseException("current lease not yet timed out"), wait_ms);
        }
    }

    public long globalToLocalTime(long globalTime) {
        return globalTime-TimeSync.getInstance().getDrift();
    }

    public void cancel(FleaseProposerCell cell, Throwable reason, long retryAfter_ms) {
        cell.addAction(ActionName.PROPOSER_CANCELLED);
        int numFailures = cell.getNumFailures() + 1;
        cell.setNumFailures(numFailures);
        LOG.debug("P proposal failed for cell {}: {}",
                    cell.getCellId(), reason);

        cell.setCellState(State.IDLE);
        cell.setMessageSent(null);
        cell.setBallotNo(new ProposalNumber(
                cell.getBallotNo().getProposalNo() + 1,
                cell.getBallotNo().getSenderId()));
        cell.getResponses().clear();

        if (numFailures > config.getMaxRetries()) {
            cell.addAction(ActionName.PROPOSER_CANCEL_LEASE_FAILED);
            FleaseException error;
            try {
                error = (FleaseException) reason;
                error.addFleaseCellDebugString(cell.toString());
            } catch (ClassCastException ex) {
                error = new FleaseException("internal flease error", reason);
            }
            leaseListener.leaseFailed(cell.getCellId(), error);
            //full lease timeout wait here...
            cell.setNumFailures(0);

            cell.addAction(ActionName.PROPOSER_SCHEDULED_RESTART);
            FleaseMessage timer = new FleaseMessage(MsgType.EVENT_RESTART);
            timer.setCellId(cell.getCellId());
            timer.setProposalNo(cell.getBallotNo());

            comm.requestTimer(timer, TimeSync.getLocalSystemTime() + config.getMaxLeaseTimeout());

        } else {
            cell.addAction(ActionName.PROPOSER_SCHEDULED_RESTART);
            FleaseMessage timer = new FleaseMessage(MsgType.EVENT_RESTART);
            final int retryTmp = (int)
                    (retryAfter_ms > 0 ? retryAfter_ms : 50 + (int) (Math.random() * 100));
            timer.setCellId(cell.getCellId());
            timer.setProposalNo(cell.getBallotNo());

            comm.requestTimer(timer, TimeSync.getLocalSystemTime() + retryTmp);

        }
    }

    private void assertState(boolean assertion, FleaseProposerCell cell) {
        if (assertion == false) {
            LOG.error("Invalid state detected. State: {}", cell);
            throw new AssertionError("Invalid state in cell "
                    + (cell != null ? cell.getCellId() : "NULL"));
        }
    }
}
