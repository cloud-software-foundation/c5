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
import io.netty.util.Timeout;
import io.netty.util.TimerTask;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws InterruptedException, SocketException {
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
        // Our message:

        Gossip.Availability.Builder builder = Gossip.Availability.newBuilder();
        builder.setNetworkPort(ourMasterPort);
        builder.setNodeId(UUID.randomUUID().toString());

        // Enumeration is hell on earth.
        for (Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces(); interfaces.hasMoreElements(); ) {
            NetworkInterface iface = interfaces.nextElement();
            for (Enumeration<InetAddress> addrs = iface.getInetAddresses(); addrs.hasMoreElements(); ) {
                InetAddress addr = addrs.nextElement();
                if (addr.isLoopbackAddress() || addr.isLinkLocalAddress() || addr.isAnyLocalAddress()) {
                    continue;
                }
                builder.addAddresses(addr.getHostAddress());
            }
        }
        final Gossip.Availability msg = builder.build();
        System.out.println("Beacon message is: " + msg);


        Bootstrap b = new Bootstrap();

        try {
            b.group(new NioEventLoopGroup())
                    .channel(NioDatagramChannel.class)
                    .option(ChannelOption.SO_BROADCAST, true)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .handler(new ElectionInit());
            // get the udp channel:
            final ChannelFuture udpChannelFuture = b.bind(port);
            udpChannelFuture.sync();
            // binded:
            wheelTimer.newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) throws Exception {
                    InetSocketAddress outAddr1 = new InetSocketAddress("255.255.255.255", port);
                    System.out.println("Sending beacon message to " + outAddr1);
                    //InetAddress outAddr = InetAddress.getByName("255.255.255.255");
                    udpChannelFuture.channel().write(new UdpProtobufEncoder.UdpProtobufMessage(outAddr1, msg));
                    //udpChannelFuture.channel().flush().sync();

                }
            }, 10, TimeUnit.SECONDS);


            udpChannelFuture.channel().closeFuture().sync();
        } finally {
            wheelTimer.stop();
            b.shutdown();
        }
    }
}
