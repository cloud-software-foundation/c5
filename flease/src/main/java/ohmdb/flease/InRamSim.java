package ohmdb.flease;

import ohmdb.flease.rpc.IncomingRpcReply;
import ohmdb.flease.rpc.OutgoingRpcRequest;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.channels.Request;
import org.jetlang.channels.RequestChannel;
import org.jetlang.core.Callback;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;

public class InRamSim {
    private static final Logger LOG = LoggerFactory.getLogger(InRamSim.class);

    final int peerSize;
    final Map<UUID, FleaseLease> fleaseRunners = new HashMap<>();
    final RequestChannel<OutgoingRpcRequest,IncomingRpcReply> rpcChannel = new MemoryRequestChannel<>();
    final Fiber rpcFiber;
    private final PoolFiberFactory fiberPool;

    public InRamSim(final int peerSize) {
        this.peerSize = peerSize;
        this.fiberPool = new PoolFiberFactory(Executors.newCachedThreadPool());

        List<UUID> peerUUIDs = new ArrayList<>();
        for (int i = 0; i < peerSize; i++) {
            peerUUIDs.add(UUID.randomUUID());
        }

        for( UUID peerId : peerUUIDs) {
            // make me a ....
            FleaseLease fl = new FleaseLease(fiberPool.create(), "lease", peerId, peerUUIDs, rpcChannel);
            fleaseRunners.put(peerId, fl);
        }

        rpcFiber = fiberPool.create();


        // subscribe to the rpcChannel:
        rpcChannel.subscribe(rpcFiber, new Callback<Request<OutgoingRpcRequest, IncomingRpcReply>>() {
            @Override
            public void onMessage(Request<OutgoingRpcRequest, IncomingRpcReply> message) {
                LOG.debug("Got a message!");

                // TODO implement me!

            }
        });

        rpcFiber.start();
    }

    public void dispose() {
        rpcFiber.dispose();
        for(FleaseLease fl : fleaseRunners.values()) {
            fl.dispose();
        }
        fiberPool.dispose();
    }
}
