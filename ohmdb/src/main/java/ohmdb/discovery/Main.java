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
package ohmdb.discovery;

import com.google.common.collect.ImmutableMap;
import org.jetlang.channels.AsyncRequest;
import org.jetlang.core.Callback;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;

import java.net.SocketException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static ohmdb.discovery.Beacon.Availability;


public class Main {
    public static void main(String[] args) throws InterruptedException, SocketException, ExecutionException {
        if (args.length < 1) {
            System.out.println("Specify cluster name as arg1 pls");
            System.exit(1);
        }
        String clusterName = args[0];

        // a non-privileged port between 1024 -> ....
        final int port = (Math.abs(clusterName.hashCode()) % 16384) + 1024;
        System.out.println("Cluster port = " + port);

        int ourMasterPort = port + (int)(Math.random() * 5000);

        Availability.Builder builder = Availability.newBuilder();
        builder.setNetworkPort(ourMasterPort);
        builder.setNodeId(UUID.randomUUID().toString());


        final BeaconService beaconService = new BeaconService(port, builder.buildPartial());
        beaconService.startAndWait();

        System.out.println("Started");

//        Thread.sleep(10000);

//        System.out.println("making state request to beacon service");
        // now try to RPC myself a tad:
        final Fiber fiber = new ThreadFiber();
        fiber.start();

        fiber.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                AsyncRequest.withOneReply(fiber, beaconService.stateRequests, 1, new Callback<ImmutableMap<String, BeaconService.NodeInfo>>() {
                    @Override
                    public void onMessage(ImmutableMap<String, BeaconService.NodeInfo> message) {
                        System.out.println("State info:");
                        for(BeaconService.NodeInfo info : message.values()) {
                            System.out.println(info);
                        }
                    }
                });

            }
        }, 10, 10, TimeUnit.SECONDS);
    }
}
