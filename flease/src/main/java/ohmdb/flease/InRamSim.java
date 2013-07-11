package ohmdb.flease;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
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
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

public class InRamSim {


    public static class Info implements InformationInterface {
        private final long plusMillis;

        public Info(long plusMillis) {
            this.plusMillis = plusMillis;
        }

        @Override
        public long currentTimeMillis() {
            return System.currentTimeMillis() + plusMillis;
        }

        @Override
        public long getLeaseLength() {
            return 10 * 1000;
        }

        @Override
        public long getEpsilon() {
            return 2 * 1000;
        }
    }
    private static final Logger LOG = LoggerFactory.getLogger(InRamSim.class);

    final int peerSize;
    final Map<Long, FleaseLease> fleaseRunners = new HashMap<>();
    final RequestChannel<OutgoingRpcRequest,IncomingRpcReply> rpcChannel = new MemoryRequestChannel<>();
    final Fiber rpcFiber;
    final List<Long> peerUUIDs = new ArrayList<>();
    private final PoolFiberFactory fiberPool;
    private final MetricRegistry metrics = new MetricRegistry();

    public InRamSim(final int peerSize) {
        this.peerSize = peerSize;
        this.fiberPool = new PoolFiberFactory(Executors.newCachedThreadPool());
        Random r = new Random();

        for (int i = 0; i < peerSize; i++) {
            peerUUIDs.add((long)r.nextInt());
        }

        long plusMillis = 0;
        for( long peerId : peerUUIDs) {
            // make me a ....
            FleaseLease fl = new FleaseLease(fiberPool.create(), new Info(plusMillis), ""+peerId, "lease", peerId, peerUUIDs, rpcChannel);
            fleaseRunners.put(peerId, fl);
            plusMillis += 500;
        }

        rpcFiber = fiberPool.create();


        // subscribe to the rpcChannel:
        rpcChannel.subscribe(rpcFiber, new Callback<Request<OutgoingRpcRequest, IncomingRpcReply>>() {
            @Override
            public void onMessage(Request<OutgoingRpcRequest, IncomingRpcReply> message) {
                messageForwarder(message);
            }
        });

        rpcFiber.start();
    }


    private final Meter messages = metrics.meter(name(InRamSim.class, "messageRate"));
    private final Counter messageTxn = metrics.counter(name(InRamSim.class, "messageTxn"));
//    private final Histogram msgSize = metrics.histogram(name(InRamSim.class, "messageBytes"));

    private void messageForwarder(final Request<OutgoingRpcRequest, IncomingRpcReply> origMsg) {

        // ok, who sent this?!!!!!
        final OutgoingRpcRequest request = origMsg.getRequest();
//        msgSize.update(request.message.getSerializedSize());
        final long dest = request.to;
        // find it:
        final FleaseLease fl = fleaseRunners.get(dest);
        if (fl == null) {
            // boo
            LOG.error("Request to non exist: " + dest);
            origMsg.reply(null);
            return;
        }

        messages.mark();
        messageTxn.inc();

        //LOG.debug("Forwarding message from {} to {}, contents: {}", request.from, request.to, request.message);
        // Construct and send a IncomingRpcRequest from the OutgoingRpcRequest.
        // There is absolutely no way to know who this is from at this point from the infrastructure.
        final IncomingRpcRequest newRequest = new IncomingRpcRequest(1, request.from, request.message);
        AsyncRequest.withOneReply(rpcFiber, fl.getIncomingChannel(), newRequest, new Callback<OutgoingRpcReply>() {
            @Override
            public void onMessage(OutgoingRpcReply msg) {
                // Translate the OutgoingRpcReply -> IncomingRpcReply.
                //LOG.debug("Forwarding reply message from {} back to {}, contents: {}", dest, request.to, msg.message);
                messages.mark();
                messageTxn.dec();
//                msgSize.update(msg.message.getSerializedSize());
                IncomingRpcReply newReply = new IncomingRpcReply(msg.message, dest);
                origMsg.reply(newReply);
            }
        });
    }

    public void run() throws ExecutionException, InterruptedException {
        FleaseLease theOneIKilled = null;

        for(int i = 0 ; i < 15 ; i++) {
            Thread.sleep(3 * 1000);

            for (FleaseLease fl : fleaseRunners.values()) {
                if (theOneIKilled != null && theOneIKilled.getId() == fl.getId()) {
                    if (i > 10)
                        System.out.print("BACK_TO_LIFE: ");
                    else if (i > 5)
                        System.out.print("DEAD: ");
                }
                if (fl.isLeaseOwner()) System.out.print("OWNER: ");
                System.out.print("Lease for " + fl.getId() + " is: " + fl.getLeaseValue());
                System.out.println(" k: " + fl.getWriteBallot());
            }

            if (i == 5) {
                for (FleaseLease fl : fleaseRunners.values()) {
                    if (fl.isLeaseOwner()) {
                        fl.dispose();
                        theOneIKilled = fl;
                    }

                }
            }
            if (i == 10) {
                // crash recovery with same UUID:
                FleaseLease newLease = new FleaseLease(fiberPool.create(), theOneIKilled.info,
                        ""+ theOneIKilled.getId(), "lease", theOneIKilled.myId, peerUUIDs, rpcChannel);
                fleaseRunners.put(theOneIKilled.myId, newLease);
            }
        }
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
        ConsoleReporter reporter = ConsoleReporter.forRegistry(sim.metrics)
                .convertDurationsTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();

        reporter.start(5, TimeUnit.SECONDS);
        sim.run();
        System.out.println("All done baby");
        sim.dispose();
        reporter.report();
        reporter.stop();
    }
}
