package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.pattern.Patterns;
import it.unitn.ds.cs.messages.*;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public class Replica extends AbstractReplica {
    Map<Integer, ActorRef> replicas = new HashMap<>(AbstractReplica.POSITIONS_LIST_LENGTH);
    int coordinatorId;
    int[] positions; //maybe do an hashmap
    const int receivedAcks;

    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
        // TODO: implement
        this.positions = new int[AbstractReplica.POSITIONS_LIST_LENGTH];
    }

    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, ActorRef listener) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
    }

    // TODO: implement
    // - Return the size of the replica group
    @Override
    public int getSystemNumberOfActors() {
        return this.replicas.size();
    }

    @Override
    public void crash(AbstractReplica.Crash how_to_crash) {
        // TODO: implement
        // - Handle the different crash types
        // - Use message counters to trigger crashes at specific points
        // - Once crashed, ignore all incoming messages

    }

    // TODO: implement
    // - Store the group of replicas
    // - Initialize the positions array
    // - Set initial epoch and sequence number
    @Override
    public void initSystem(InitSystem sysInit) {
        this.replicas = sysInit.group;
        this.coordinatorId = sysInit.coordinator_id;
        if (this.id == this.coordinatorId) {
            getContext().become(coordinator());
        }
    }

    // TODO: handle read requests by clients
    // - Return current value at index from positions array
    // - Send ReadResult back to client
    public void handleReadRequest(CSRead msg) {
        try {
            int result = this.positions[msg.index];
            getSender().tell(new CSReadResult(true, msg.index, result, this.id), getSelf());
        } catch (IndexOutOfBoundsException e) {
            getSender().tell(new CSReadResult(false, msg.index, 0, this.id), getSelf());
        }
    }

    // TODO: handle write requests by clients
    // - If this is coordinator: start update protocol
    // - If not: forward to current coordinator
    public void handleWriteRequest(CSWrite msg) {
        Duration timeout = Duration.ofMillis(AbstractReplica.MAX_LATENCY * 2);
        CompletionStage<Object> future = Patterns.ask(replicas.get(this.coordinatorId), msg, timeout);

        future.exceptionally(e -> {
            election();

            return null;
        });
    }

    public void handleWriteRequestCoordinator(CSWrite msg) {
        getSender().tell(new CSAck(), getSelf());



        //TOOD update
    }

    // TODO: implement 2-phase commit:
    // - Coordinator sends UPDATE to all replicas
    // - Replicas send ACK
    // - Coordinator waits for quorum |_N/2_| +1
    // - Coordinator broadcasts WRITEOK
    // - Replicas apply update on WRITEOK
    public final void update() {
        Duration timeout = Duration.ofMillis(AbstractReplica.MAX_LATENCY * 2);

        for (var replica: replicas.entrySet()) {
            CompletionStage<Object> future = Patterns.ask(replicas.get(this.coordinatorId), msg, timeout);

            future.thenAccept(result -> {
                this.receivedAcks += 1;
                if (this.receivedAcks == this.replicas.size() / 2 + 1) {
                    // writeok
                }
            }).exceptionally(e -> {
                this.replicas.remove(replica.getKey());

                return null;
            });
        }
    }

    // TODO: implement coordinator crash detection
    // - Coordinator multicasts heartbeat messages
    // - Replicas monitor heartbeat
    // - Coordinator election is triggered if heartbeat is not received or if expected messages are not received

    // TODO: implement coordinator election
    // - When crash detected: send ELECTION message to next replica
    // - Each replica adds its knowledge of the latest update and forwards
    // - If timeout: skip crashed replica and forward to next
    // - When the ring is completed, forward again to the new coordinator (Replica with most recent update wins, break ties with replica ID)
    // - New coordinator sends SYNCHRONIZATION and replicas update their coordinator reference
    // - Sends any missing updates to other replicas
    public final void election() {

    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(CSRead.class, this::handleReadRequest)
                .match(CSWrite.class, this::handleWriteRequest)
                .build();
    }

    public final Receive coordinator() {
        return receiveBuilder()
                .match(CSRead.class, this::handleReadRequest)
                .match(CSWrite.class, this::handleWriteRequestCoordinator)
                .build();
    }

}
