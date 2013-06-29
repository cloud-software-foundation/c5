package ohmdb.flease;

import com.google.common.util.concurrent.ListenableFuture;
import ohmdb.flease.rpc.IncomingRpcReply;
import ohmdb.flease.rpc.IncomingRpcRequest;
import ohmdb.flease.rpc.OutgoingRpcReply;
import ohmdb.flease.rpc.OutgoingRpcRequest;
import org.jetlang.channels.AsyncRequest;
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
import java.util.concurrent.ExecutionException;
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

                doThing(message);
            }
        });

        rpcFiber.start();
    }

    private void doThing(final Request<OutgoingRpcRequest, IncomingRpcReply> origMsg) {

        // ok, who sent this?!!!!!
        final OutgoingRpcRequest request = origMsg.getRequest();
        final UUID dest = request.to;
        // find it:
        final FleaseLease fl = fleaseRunners.get(dest);
        if (fl == null) {
            // boo
            LOG.error("Request to non exist: " + dest);
            origMsg.reply(null);
            return;
        }

        LOG.debug("Forwarding message from {} to {}, contents: {}", request.from, request.to, request.message);
        // Construct and send a IncomingRpcRequest from the OutgoingRpcRequest.
        // There is absolutely no way to know who this is from at this point from the infrastructure.
        final IncomingRpcRequest newRequest = new IncomingRpcRequest(1, request.from, request.message);
        AsyncRequest.withOneReply(rpcFiber, fl.getIncomingChannel(), newRequest, new Callback<OutgoingRpcReply>() {
            @Override
            public void onMessage(OutgoingRpcReply msg) {
                // Translate the OutgoingRpcReply -> IncomingRpcReply.
                LOG.debug("Forwarding reply message from {} back to {}, contents: {}", dest, request.to, msg.message);
                IncomingRpcReply newReply = new IncomingRpcReply(msg.message, dest);
                origMsg.reply(newReply);
            }
        });
    }

    public void run() throws ExecutionException, InterruptedException {
        FleaseLease fl = fleaseRunners.values().iterator().next();
        ListenableFuture<LeaseValue> future = fl.read();
        //LOG.info("read result: {}", future.get());
        LeaseValue result = future.get();
        System.out.println("read result: " + result);

        String datum = "";
        if (result.isBefore(System.currentTimeMillis())) {
            // new lease!
            datum = "new lease";
        } else {
            datum = result.datum;
        }
        ListenableFuture<LeaseValue> writeFuture = fl.write(datum);
        System.out.println("write value: " + writeFuture.get());
    }

    public void dispose() {
        rpcFiber.dispose();
        for(FleaseLease fl : fleaseRunners.values()) {
            fl.dispose();
        }
        fiberPool.dispose();
    }

    public static void main(String []args) throws ExecutionException, InterruptedException {
        InRamSim sim = new InRamSim(3);
        sim.run();
        sim.dispose();
    }
}
