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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.HashedWheelTimer;

import java.net.SocketException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws InterruptedException, SocketException, ExecutionException {
        HashedWheelTimer wheelTimer = new HashedWheelTimer(1, TimeUnit.SECONDS);

        if (args.length < 1) {
            System.out.println("Specify cluster name as arg1 pls");
            System.exit(1);
        }
        String clusterName = args[0];

        // a non-privileged port between 1024 -> ....
        final int port = (Math.abs(clusterName.hashCode()) % 16384) + 1024;
        System.out.println("Cluster port = " + port);

        int ourMasterPort = port + (int)(Math.random() * 5000);

        Gossip.Availability.Builder builder = Gossip.Availability.newBuilder();
        builder.setNetworkPort(ourMasterPort);
        builder.setNodeId(UUID.randomUUID().toString());


        Bootstrap b = new Bootstrap();
        BeaconService beaconService = null;

        try {
            b.group(new NioEventLoopGroup(1))
                    .channel(NioDatagramChannel.class)
                    .option(ChannelOption.SO_BROADCAST, true)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .handler(new ElectionInit());
            // get the udp channel:
            final ChannelFuture udpChannelFuture = b.bind(port);
            udpChannelFuture.sync();

            // Start the beacon service:
            beaconService = new BeaconService(udpChannelFuture.channel(), builder.buildPartial());
            beaconService.start().get();

            udpChannelFuture.channel().closeFuture().sync();
        } finally {
            if (beaconService != null) {
                beaconService.stopAndWait();
            }
            wheelTimer.stop();
            b.shutdown();
        }
    }
}
