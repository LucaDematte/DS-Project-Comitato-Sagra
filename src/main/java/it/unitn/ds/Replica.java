package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.pattern.Patterns;
import it.unitn.ds.cs.CSUpdateKey;
import it.unitn.ds.cs.CSUpdateValue;
import it.unitn.ds.cs.messages.*;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionStage;

public class Replica extends AbstractReplica {
    // Replica
    Map<Integer, ActorRef> replicas = new HashMap<>(AbstractReplica.POSITIONS_LIST_LENGTH);
    int previous, next;
    int coordinatorId;
    Map<UUID, ActorRef> clientsRequests = new HashMap<>();
    Map<CSUpdateKey, UUID> uuidBindings = new HashMap<>();
    int[] positions; //maybe do a hashmap
    Map<CSUpdateKey, CSUpdateValue> updates = new HashMap<>();

    // Coordinator
    CSUpdateKey updateKey;
    int receivedAcks;
    Queue<CSWriteForward> queue = new LinkedList<>();
    boolean processing;

    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
        this.positions = new int[AbstractReplica.POSITIONS_LIST_LENGTH];
        this.updateKey = new CSUpdateKey(0, 0);
        this.processing = false;
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
        return this.replicas.size();
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
        this.replicas = sysInit.group;
        this.coordinatorId = sysInit.coordinator_id;

        this.previous = (this.id - 1) % this.replicas.size();
        this.next = (this.id + 1) % this.replicas.size();

        if (this.id == this.coordinatorId) {
            getContext().become(coordinator());
        }
    }

    public void handleReadRequest(CSReadRequest msg) {
        try {
            int result = this.positions[msg.index];
            getSender().tell(new CSReadResult(true, msg.index, result, this.id), getSelf());
        } catch (IndexOutOfBoundsException e) {
            getSender().tell(new CSReadResult(false, msg.index, 0, this.id), getSelf());
        }
    }

    public void handleWriteRequest(CSWriteRequest msg) {
        UUID uuid = UUID.randomUUID();
        this.clientsRequests.put(uuid, getSender());

        Duration timeout = Duration.ofMillis(AbstractReplica.MAX_LATENCY * 2);
        CompletionStage<Object> future = Patterns.ask(replicas.get(this.coordinatorId), new CSWriteForward(msg, uuid), timeout);

        future.exceptionally(e -> {
            election();

            return null;
        });
    }

    public void handleWriteRequestCoordinator(CSWriteRequest msg) {
//        log("Write Request Received from" + this.id);

        UUID uuid = UUID.randomUUID();
        this.clientsRequests.put(uuid, getSender());

        handleWriteForward(new CSWriteForward(msg, uuid));
    }

    public void handleWriteForward(CSWriteForward msg) {
        this.queue.add(msg);
        getSender().tell(new CSAck(), getSelf());

        if (!this.processing) {
            this.processing = true;

//            log("Processing queue");
            while (!this.queue.isEmpty()) {
                CSWriteForward current = this.queue.poll();
                this.receivedAcks += 1; // coordinator immediately acks to itself
                update(current);
            }

            this.processing = false;
        }
    }

    public final void update(CSWriteForward msg) {
        CSUpdateKey tmp = new CSUpdateKey(this.updateKey);

        Duration timeout = Duration.ofMillis(AbstractReplica.MAX_LATENCY * 2);
        multicast(new CSUpdate(tmp, new CSUpdateValue(msg.request().index, msg.request().value, false), msg.uuid()), timeout, Optional.empty(),
                (replica_id, res, e) -> {
                    if (e == null) {
//                        log("Received ack from: " + replica_id);
                        this.receivedAcks += 1;
                        if (receivedAcks == this.replicas.size() / 2 + 1) { // slash with integers always floor
                            // happens only once because of ==
                            this.positions[msg.request().index] = msg.request().value;
                            multicast(new CSWriteOk(tmp), Optional.empty());
                            this.receivedAcks = 0;

                            callbackOnUpdateApplied(msg.request().index, msg.request().value);
                        }
                    } else {
                        int crashed_replica_id = replica_id;
                        this.replicas.remove(crashed_replica_id);

                        int previous_replica_id = this.replicas.keySet().stream().filter(id -> id < crashed_replica_id).max(Integer::compare).orElse(Collections.min(this.replicas.keySet()));
                        int next_replica_id =
                                this.replicas.keySet().stream().filter(id -> id > crashed_replica_id).min(Integer::compare).orElse(Collections.max(this.replicas.keySet()));
                        this.replicas.get(previous_replica_id).tell(new CSCrashNotice(crashed_replica_id, previous_replica_id
                                , next_replica_id), getSelf());
                        this.replicas.get(next_replica_id).tell(new CSCrashNotice(crashed_replica_id, previous_replica_id, next_replica_id),
                                getSelf());
                    }
                }
        );
        this.updateKey = new CSUpdateKey(this.updateKey.epoch(), this.updateKey.seq_no() + 1); //TODO check if final is
        // needed
    }

    public final void handleUpdate(CSUpdate msg) {
//        log("Received Update with key:" + msg.key());
        this.updates.put(msg.key(), msg.update());
        this.uuidBindings.put(msg.key(), msg.uuid());
        getSender().tell(new CSAck(), getSelf());
    }

    public final void handleWriteOk(CSWriteOk msg) {
//        log("Received WriteOk with key:" + msg.key);

        CSUpdateValue tmp = this.updates.get(msg.key);
        positions[tmp.index()] = tmp.value();
        this.updates.replace(msg.key, new CSUpdateValue(tmp.index(), tmp.value(), true));

        var uuid = this.uuidBindings.get(msg.key);
        if (this.clientsRequests.containsKey(uuid)) {
            this.clientsRequests.get(uuid).tell(new CSAck(), getSelf());
            this.clientsRequests.remove(uuid);
        }
        this.uuidBindings.remove(msg.key);

        // testing
        callbackOnUpdateApplied(tmp.index(), tmp.value());
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

    @FunctionalInterface
    interface ReplicaHandler {
        void handle(Integer replica_id, Object result, Throwable error);
    }

    public final void multicast(Serializable msg, Duration timeout, Optional<Integer> crash_message_n, ReplicaHandler handler) {
        var group = new HashMap<>(this.replicas);
        group.remove(this.coordinatorId); // multicast does not include coordinator

        int i = 0;
        for (var replica : group.entrySet()) {
            if (crash_message_n.isPresent() && i == crash_message_n.get()) {
                // crash
            }
            CompletionStage<Object> future = Patterns.ask(replica.getValue(), msg, timeout);
            future.handle((res, e) -> {
                handler.handle(replica.getKey(), res, e);
                return null;
            });
            i++;
        }
    }

    public final void multicast(Serializable msg, Optional<Integer> crash_message_n) {
        var group = new HashMap<>(this.replicas);
        group.remove(this.coordinatorId); // multicast does not include coordinator

        int i = 0;
        for (var replica : group.entrySet()) {
            if (crash_message_n.isPresent() && i == crash_message_n.get()) {
                // crash
            }
            replica.getValue().tell(msg, getSelf());
        }
    }

    public final void handleCrashNotice(CSCrashNotice msg) {
        if (this.id < msg.crashed_id) {
            this.next = msg.next;
        } else {
            this.previous = msg.previous;
        }
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(CSReadRequest.class, this::handleReadRequest)
                .match(CSWriteRequest.class, this::handleWriteRequest)
                .match(CSCrashNotice.class, this::handleCrashNotice)
                .match(CSUpdate.class, this::handleUpdate)
                .match(CSWriteOk.class, this::handleWriteOk)
                .build();
    }

    public final Receive coordinator() {
        return receiveBuilder()
                .match(CSReadRequest.class, this::handleReadRequest)
                .match(CSWriteRequest.class, this::handleWriteRequestCoordinator)
                .match(CSWriteForward.class, this::handleWriteForward)
//                .match(CSUpdate.class, this::handleUpdate)
//                .match(CSWriteOk.class, this::handleWriteOk)
                .build();
    }

}
