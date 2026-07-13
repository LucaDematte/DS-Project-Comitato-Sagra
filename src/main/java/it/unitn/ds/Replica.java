package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds.cs.CSAsk;
import it.unitn.ds.cs.CSLogger;
import it.unitn.ds.cs.CSUpdateData;
import it.unitn.ds.cs.CSUpdateKey;
import it.unitn.ds.cs.messages.CSAskMessage;
import it.unitn.ds.cs.messages.CSAskTimeout;
import it.unitn.ds.cs.messages.client.CSReadRequest;
import it.unitn.ds.cs.messages.client.CSWriteRequest;
import it.unitn.ds.cs.messages.coordinator.CSCrashNotice;
import it.unitn.ds.cs.messages.coordinator.CSUpdate;
import it.unitn.ds.cs.messages.coordinator.CSWriteOk;
import it.unitn.ds.cs.messages.replica.CSAck;
import it.unitn.ds.cs.messages.replica.CSReadResult;
import it.unitn.ds.cs.messages.replica.CSWriteForward;
import it.unitn.ds.cs.messages.replica.CSWriteResult;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;

public class Replica extends AbstractReplica {
    // Replica
    Map<Integer, ActorRef> replicas = new HashMap<>(AbstractReplica.POSITIONS_LIST_LENGTH);
    int previous, next;
    int coordinatorId;
    int[] positions; //maybe do a hashmap
    final CSLogger logger = new CSLogger();

    // Coordinator
    CSUpdateKey updateKey;
    Map<CSUpdateKey, Integer> receivedAcks = new HashMap<>();
    /**
     * The queue is of CSWriteForward instead of CSWriteRequest
     * Because additional information is needed and are inserted in CSWriteForward
     * to leave the implementation of CSWriteRequest clean
     */
    Queue<CSWriteForward> queue = new LinkedList<>();
    boolean processing;

    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY,
                AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
    }

    public Replica(
            int id, int minLatency, int maxLatency, int coordinatorBeatInterval,
            Optional<ActorRef> listener
    ) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
        this.positions = new int[AbstractReplica.POSITIONS_LIST_LENGTH];
        this.updateKey = new CSUpdateKey(0, 0);
        this.processing = false;
    }

    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(Replica.class,
                () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval,
                        Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(
            int id, int minLatency, int maxLatency, int coordinatorBeatInterval,
            ActorRef listener
    ) {
        return Props.create(Replica.class,
                () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval,
                        Optional.ofNullable(listener)));
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

    private void handleReadRequest(CSReadRequest msg) {
        try {
            int result = this.positions[msg.index];
            tell(new CSReadResult(true, msg.index, result, this.id, msg.askUUID), getSender());
        } catch (IndexOutOfBoundsException e) {
            tell(new CSReadResult(false, msg.index, 0, this.id, msg.askUUID), getSender());
        }
    }

    private void handleWriteRequest(CSWriteRequest msg) {
        UUID writeRequestUUID = UUID.randomUUID();
        this.logger.logRequest(writeRequestUUID, new CSUpdateData(msg.index, msg.value, false), getSender(),
                this.id, msg.askUUID);

        super.log("Adding new local update coming from client " + getSender().path()
                .name() + " with uuid " + writeRequestUUID);

        Duration timeout = Duration.ofMillis(super.getMaxLatencyPlusTolerance());

        CSAsk.<CSUpdate>ask(getContext(), super::tell, new CSWriteForward(msg, writeRequestUUID), replicas.get(this.coordinatorId),
                timeout, (res, timedOut) -> {
                    if (timedOut) {
                        election();
                    }
                });
    }

    private void handleWriteRequestCoordinator(CSWriteRequest msg) {
        UUID writeRequestUUID = UUID.randomUUID();
        this.logger.logRequest(writeRequestUUID, new CSUpdateData(msg.index, msg.value, false), getSender(),
                this.id, msg.askUUID);

        super.log("Adding new local update coming from client " + getSender().path()
                .name() + " with uuid " + writeRequestUUID);
        super.log("Adding update to queue");

        this.queue.add(new CSWriteForward(msg, writeRequestUUID));
        processUpdates();
    }

    private void handleWriteForward(CSWriteForward msg) {
        super.log("Adding update to queue");
        this.queue.add(msg);
        super.tell(new CSAck(msg.askUUID), getSender());
        processUpdates();
    }

    private void handleUpdate(CSUpdate msg) {
        super.log("Received Update with key:" + msg.key);
        this.logger.logUpdate(msg.key, msg.writeRequestUUID, msg.data);
        super.tell(new CSAck(msg.askUUID), getSender());
    }

    // =================================================================================
    // Update Protocol
    // =================================================================================

    private void processUpdates() {
        if (!this.processing) {
            if (!this.queue.isEmpty()) {
                this.processing = true;
                var current = this.queue.poll();
                super.log("Starting processing a message");
                this.update(current);
            } else {
                super.log("No more messages to process");
            }
        } else {
            super.log("Already processing a message");
        }
    }

    private void update(CSWriteForward msg) {
        // Deep copy to avoid working on actor state
        CSUpdateKey updateKey = new CSUpdateKey(this.updateKey);

        this.receivedAcks.put(updateKey, 1); // coordinator immediately acks to itself

        Duration timeout = Duration.ofMillis(super.getMaxLatencyPlusTolerance());
        multicast(new CSUpdate(updateKey, new CSUpdateData(msg.request.index, msg.request.value, false),
                        msg.writeRequestUUID,
                        msg.askUUID), timeout,
                Optional.empty(), (replicaId, res, timedOut) -> {
                    if (!timedOut) {
                        super.log("Received ack from: " + replicaId);
                        this.receivedAcks.put(updateKey, this.receivedAcks.get(updateKey) + 1);

                        // slash with integers always floor
                        if (this.receivedAcks.get(updateKey) == this.replicas.size() / 2 + 1) { // happens only once because of ==
                            super.log("Quorum reached for update " + updateKey + ", sending WriteOk to replicas");
                            multicast(new CSWriteOk(updateKey), Optional.empty());
                            this.completeUpdate(updateKey);
                            this.processing = false;
                            processUpdates(); // continue processing untile queue is empty
                        }
                    } else {
                        int crashed_replica_id = replicaId;

                        super.log("Replica with ID " + crashed_replica_id + " crashed! Sending notices to replicas");

                        // TODO removing replicas from the list causes problems with the quorum calculation
                        this.replicas.remove(crashed_replica_id);

                        int previous_replica_id = this.replicas.keySet()
                                .stream()
                                .filter(id -> id < crashed_replica_id)
                                .max(Integer::compare)
                                .orElse(Collections.min(
                                        this.replicas.keySet()));
                        int next_replica_id = this.replicas.keySet()
                                .stream()
                                .filter(id -> id > crashed_replica_id)
                                .min(Integer::compare)
                                .orElse(Collections.max(
                                        this.replicas.keySet()));
                        this.replicas.get(previous_replica_id)
                                .tell(new CSCrashNotice(crashed_replica_id,
                                        previous_replica_id, next_replica_id), getSelf());
                        this.replicas.get(next_replica_id)
                                .tell(new CSCrashNotice(crashed_replica_id,
                                        previous_replica_id, next_replica_id), getSelf());
                    }
                });
        this.updateKey = new CSUpdateKey(this.updateKey.epoch,
                this.updateKey.seq_no + 1);
    }

    public final void handleWriteOk(CSWriteOk msg) {
        super.log("Received WriteOk with key:" + msg.key);

        this.completeUpdate(msg.key);
        this.sendWriteResult(msg.key);
    }

    public void completeUpdate(CSUpdateKey key) {
        super.log("Writing to positions");
        CSUpdateData update = this.logger.getUpdateData(key);
        this.positions[update.index] = update.value;
        callbackOnUpdateApplied(update.index, update.value);
        this.logger.setCompleted(key);
    }

    public void sendWriteResult(CSUpdateKey key) {
        CSUpdateData update = this.logger.getUpdateData(key);
        CSLogger.CSClientData client = this.logger.getClientData(key);
        if (this.id == client.contactedReplicaId()) { // TODO: is there any better way of doing this?
            super.log("Sending WriteResult to client " + client.actor().toString());
            super.tell(new CSWriteResult(update.index, update.value, true, this.id, client.askUUID()), client.actor());
        } else {
            super.log("I'm not the original handler of this update, no need to send result to any client");
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

    @FunctionalInterface
    interface ReplicaHandler {
        void handle(Integer replica_id, Object result, boolean timedOut);
    }

    public final void multicast(CSAskMessage msg, Duration timeout, Optional<Integer> crash_message_n,
                                ReplicaHandler handler
    ) {
        var group = new HashMap<>(this.replicas);
        group.remove(this.coordinatorId); // multicast does not include coordinator

        int i = 0;
        for (var replica : group.entrySet()) {
            if (crash_message_n.isPresent() && i == crash_message_n.get()) {
                // crash
            }

            CSAsk.ask(getContext(), super::tell, msg, replica.getValue(), timeout, (res, timedOut) -> {
                handler.handle(replica.getKey(), res, timedOut);
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
            tell(msg, replica.getValue());
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

                // ask handlers
                .match(CSAskTimeout.class, CSAsk::handleTimeout)
                .build();
    }

    public final Receive coordinator() {
        return receiveBuilder()
                .match(CSReadRequest.class, this::handleReadRequest)
                .match(CSWriteRequest.class, this::handleWriteRequestCoordinator)
                .match(CSWriteForward.class, this::handleWriteForward)

                // ask handlers
                .match(CSAck.class, CSAsk::handleResponse)
                .match(CSAskTimeout.class, CSAsk::handleTimeout)
                .build();
    }

}
