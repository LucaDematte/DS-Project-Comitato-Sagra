package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.Optional;

public class Replica extends AbstractReplica {

    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
        // TODO: implement
    }

    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, ActorRef listener) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
    }

    @Override
    public int getSystemNumberOfActors() {
        // TODO: implement
        // - Return the size of the replica group

        return 0;
    }

    @Override
    public void crash(AbstractReplica.Crash how_to_crash) {
        // TODO: implement
        // - Handle the different crash types
        // - Use message counters to trigger crashes at specific points
        // - Once crashed, ignore all incoming messages

    }

    @Override
    public void initSystem(InitSystem sysInit) {
        // TODO: implement
        // - Store the group of replicas
        // - Initialize the positions array
        // - Set initial epoch and sequence number

    }

    // TODO: handle read requests by clients
    // - Return current value at index from positions array
    // - Send ReadResult back to client

    // TODO: handle write requests by clients
    // - If this is coordinator: start update protocol
    // - If not: forward to current coordinator

    // TODO: implement 2-phase commit:
    // - Coordinator sends UPDATE to all replicas
    // - Replicas send ACK
    // - Coordinator waits for quorum |_N/2_| +1
    // - Coordinator broadcasts WRITEOK
    // - Replicas apply update on WRITEOK

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

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                // TODO add your message handlers here .match(, )
                .build();
    }

}
