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
 */
package ohmdb.election;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.SettableFuture;
import ohmdb.discovery.Beacon;
import ohmdb.discovery.BeaconService;
import org.jetlang.channels.AsyncRequest;
import org.jetlang.core.Callback;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xtreemfs.foundation.TimeSync;
import org.xtreemfs.foundation.buffer.ASCIIString;
import org.xtreemfs.foundation.flease.Flease;
import org.xtreemfs.foundation.flease.FleaseConfig;
import org.xtreemfs.foundation.flease.FleaseStatusListener;
import org.xtreemfs.foundation.flease.FleaseViewChangeListenerInterface;
import org.xtreemfs.foundation.flease.comm.tcp.TCPFleaseCommunicator;
import org.xtreemfs.foundation.flease.proposer.FleaseException;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static ohmdb.discovery.BeaconService.NodeInfo;

public class FleaseTestMain implements FleaseStatusListener, FleaseViewChangeListenerInterface {
    private static final Logger LOG = LoggerFactory.getLogger(FleaseTestMain.class);

    private final TCPFleaseCommunicator tcpFlease;
    private final FleaseConfig fleaseConfig;
    private final RamMasterEpochHandler ramMasterEpochHandler = new RamMasterEpochHandler();
    private final String clusterName;
    private final int fleasePort;
    private final Beacon.Availability nodeInfoTemplate;
    private final BeaconService beaconService;
    private final int discoveryPort;
    private final String identity;


    public FleaseTestMain(int discoveryPort, String clusterName, int ourMasterPort, Beacon.Availability.Builder builder) throws Exception {
        // these magic values are yanked from FleaseSim.  We can probably do better.
        this.discoveryPort = discoveryPort;
        this.clusterName = clusterName;
        this.fleasePort = ourMasterPort;
        this.nodeInfoTemplate = builder.build();

        // who am i:
        this.identity = "localhost:" + this.fleasePort;
        this.fleaseConfig =
                new FleaseConfig(5000, 500, 500, new InetSocketAddress(this.fleasePort), identity, 5, true, 0);

        // use TCPFleaseCommunicator
        this.tcpFlease = new TCPFleaseCommunicator(fleaseConfig, "/tmp/ryan/", true,
                this, this, ramMasterEpochHandler);

        this.beaconService = new BeaconService(discoveryPort, nodeInfoTemplate);

    }

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("provide cluster name on args");
            System.exit(1);
        }

        String clusterName = args[0];

        // a non-privileged port between 1024 -> ....
        final int port = (Math.abs(clusterName.hashCode()) % 16384) + 1024;
        System.out.println("Cluster port = " + port);

        int ourMasterPort = port + (int)(Math.random() * 5000);

        Beacon.Availability.Builder builder = Beacon.Availability.newBuilder();
        builder.setBaseNetworkPort(ourMasterPort);
        builder.setNodeId(new Random().nextLong());

        TimeSync.initializeLocal(50);

        new FleaseTestMain(port, clusterName, ourMasterPort, builder).run();

        // 7 argument constructors drive me nuts.
        // sender, viewListener, leaseListener, masterEpochHandler

    }

    private void run() throws Exception {
        this.tcpFlease.start();
        this.beaconService.start();

        // we need some peers to peer with.
        ImmutableMap <Long,NodeInfo> peers = waitForAPeerOrMore();
        List<InetSocketAddress> peerAddrs = new ArrayList<>(peers.size());
        for (NodeInfo nodeInfo : peers.values()) {
            if (!(nodeInfo.availability.getNodeId() == nodeInfoTemplate.getNodeId())) {
                // just use first IP:
                peerAddrs.add(new InetSocketAddress(nodeInfo.availability.getAddresses(0), nodeInfo.availability.getBaseNetworkPort()));
            }
        }

        // now something something dark side
        tcpFlease.getStage().openCell(new ASCIIString(this.clusterName), peerAddrs, false);
    }

    private ImmutableMap<Long,NodeInfo> waitForAPeerOrMore() throws ExecutionException, InterruptedException {
        final Fiber fiber = new ThreadFiber();
        fiber.start();
        final SettableFuture<ImmutableMap<Long,NodeInfo>> future = SettableFuture.create();

        fiber.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                AsyncRequest.withOneReply(fiber, beaconService.stateRequests, 1, new Callback<ImmutableMap<Long, NodeInfo>>() {
                    @Override
                    public void onMessage(ImmutableMap<Long, NodeInfo> message) {
                        if (message.size() >= 2) {
                            // yay...
                            LOG.info("Got more than 3 peers, continuing");
                            future.set(message);
                            fiber.dispose(); // end this mess.
                        }
                    }
                });
            }
        }, 2, 2, TimeUnit.SECONDS);

        LOG.info("Waiting for peers...");
        return future.get();
    }

    // FleaseStatusListenener
    @Override
    public void statusChanged(ASCIIString cellId, Flease lease) {
        if (lease.getLeaseHolder() != null && lease.getLeaseHolder().toString().equals(this.identity)) {
            LOG.info("I AM THE LEADER! FOLLOW ME!");
        }
        LOG.info("statusChanged {}/{}", cellId, lease);
    }

    @Override
    public void leaseFailed(ASCIIString cellId, FleaseException error) {
        LOG.error("leaseFailed {}/{}", cellId, error);
    }

    // FleaseViewChangeListenerInterface
    @Override
    public void viewIdChangeEvent(ASCIIString cellId, int viewId) {
        LOG.info("viewIdChangeEvent {} {}", cellId, viewId);

    }
}
