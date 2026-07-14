package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds.cs.CSAsk;
import it.unitn.ds.cs.CSUpdateData;
import it.unitn.ds.cs.CSUpdateKey;
import it.unitn.ds.cs.logger.CSClientData;
import it.unitn.ds.cs.logger.CSLogger;
import it.unitn.ds.cs.messages.CSAskMessage;
import it.unitn.ds.cs.messages.CSAskTimeout;
import it.unitn.ds.cs.messages.client.CSHeartBeatCheck;
import it.unitn.ds.cs.messages.client.CSReadRequest;
import it.unitn.ds.cs.messages.client.CSWriteRequest;
import it.unitn.ds.cs.messages.coordinator.CSCrashNotice;
import it.unitn.ds.cs.messages.coordinator.CSHeartBeatFromCoordinator;
import it.unitn.ds.cs.messages.coordinator.CSUpdate;
import it.unitn.ds.cs.messages.coordinator.CSWriteOk;
import it.unitn.ds.cs.messages.replica.CSAck;
import it.unitn.ds.cs.messages.replica.CSReadResult;
import it.unitn.ds.cs.messages.replica.CSWriteForward;
import it.unitn.ds.cs.messages.replica.CSWriteResult;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;
import java.util.function.Supplier;

public class Replica extends AbstractReplica {
    CSAsk askSystem = new CSAsk(getContext(), super::tell);
    Cancellable heartBeatTimer;

    // Replica
    Map<Integer, ActorRef> replicas = new HashMap<>(AbstractReplica.POSITIONS_LIST_LENGTH);
    int previous, next;
    int coordinatorId;
    int[] positions; //maybe do a hashmap
    final CSLogger logger = new CSLogger();
    boolean heartBeatReceived;

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
        this.replicas = new HashMap<>(sysInit.group);
        this.coordinatorId = sysInit.coordinator_id;

        this.previous = (this.id - 1) % this.replicas.size();
        this.next = (this.id + 1) % this.replicas.size();

        if (this.id == this.coordinatorId) {
            this.becomeCoordinator();
        } else {
            this.heartBeatReceived = false;
            this.heartBeatTimer = null;
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
        var clientData = new CSClientData(getSender(), this.id, msg.askUUID);
        this.logger.logRequest(writeRequestUUID, new CSUpdateData(msg.index, msg.value, false), clientData);

        super.debug("Adding new update coming from client " + getSender().path()
                .name() + " with uuid " + writeRequestUUID);

        Duration timeout = Duration.ofMillis(super.getMaxLatencyPlusTolerance());

        askSystem.<CSUpdate>ask(new CSWriteForward(msg, writeRequestUUID, clientData), replicas.get(this.coordinatorId),
                timeout, (res, timedOut) -> {
                    if (timedOut) {
                        election();
                    } else {
                        super.debug("Forward did not time out");
                    }
                });
    }

    private void handleWriteRequestCoordinator(CSWriteRequest msg) {
        UUID writeRequestUUID = UUID.randomUUID();
        super.debug("WriteRequest askUUID: " + msg.askUUID);
        var clientData = new CSClientData(getSender(), this.id, msg.askUUID);
        this.logger.logRequest(writeRequestUUID, new CSUpdateData(msg.index, msg.value, false), clientData);

        super.debug("Received write request from client: " + getSender().path()
                .name() + " - uuid: " + writeRequestUUID);

        this.queue.add(new CSWriteForward(msg, writeRequestUUID, clientData));
        super.debug("Update added to queue");
        processUpdates();
    }

    private void handleWriteForward(CSWriteForward msg) {
        super.debug("Adding update to queue");
        this.queue.add(msg);
        processUpdates();
    }

    private void handleUpdate(CSUpdate msg) {
        super.debug("Received Update with key:" + msg.key);
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
                super.debug("Starting processing an update");
                this.update(current);
            } else {
                super.debug("No more updates to process");
            }
        } else {
            super.debug("Already processing an update");
        }
    }

    private void update(CSWriteForward msg) {
        // Deep copy to avoid working on actor state
        CSUpdateKey updateKey = new CSUpdateKey(this.updateKey);

        CSUpdateData data = new CSUpdateData(msg.request.index, msg.request.value, false);
        this.logger.logUpdate(updateKey, msg.writeRequestUUID, data);

        this.receivedAcks.put(updateKey, 1); // coordinator immediately acks to itself

        var otherReplicas = new HashMap<>(this.replicas);

        otherReplicas.remove(this.coordinatorId); // same as this.id because update is executed only by the coordinator
        otherReplicas.remove(msg.clientData.contactedReplicaId);

        Duration timeout = Duration.ofMillis(2L * super.getMaxLatencyPlusTolerance());

        ReplicaHandler handler = (replicaId, res, timedOut) -> {
            if (!timedOut) {
                super.debug("Received ack from: " + replicaId);
                this.receivedAcks.put(updateKey, this.receivedAcks.get(updateKey) + 1);

                // slash with integers always floor
                if (this.receivedAcks.get(updateKey) == this.replicas.size() / 2 + 1) { // happens only once because of ==
                    super.debug("Quorum reached for update " + updateKey + ", sending WriteOk to replicas");
                    broadcast(new CSWriteOk(updateKey), Optional.empty());

                    this.completeUpdate(updateKey);
                    this.sendWriteResult(updateKey);
                    this.processing = false;
                    processUpdates(); // continue processing untile queue is empty
                }
            } else {
                int crashed_replica_id = replicaId;

                super.debug("Replica with ID " + crashed_replica_id + " crashed! Sending notices to replicas");

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
        };

        // note: coordinator never sends messages to itself
        // always handles updates by managing internal state
        if (this.coordinatorId != msg.clientData.contactedReplicaId) {
            askSystem.<CSAck>ask(new CSUpdate(updateKey, data,
                    msg.writeRequestUUID,
                    msg.askUUID), this.replicas.get(msg.clientData.contactedReplicaId), timeout, (res, timedOut) -> {
                handler.handle(msg.clientData.contactedReplicaId, res, timedOut);
            });
        }
        this.multicast(otherReplicas, () -> new CSUpdate(updateKey, data,
                        msg.writeRequestUUID), timeout,
                Optional.empty(), handler);
        super.debug("Sent update" + updateKey + " to replicas");

        this.updateKey = new CSUpdateKey(this.updateKey.epoch,
                this.updateKey.seq_no + 1);
    }

    public final void handleWriteOk(CSWriteOk msg) {
        super.debug("Received WriteOk with key:" + msg.key);

        this.completeUpdate(msg.key);
        this.sendWriteResult(msg.key);
    }

    public void completeUpdate(CSUpdateKey key) {
        super.debug("Writing to positions");
        CSUpdateData update = this.logger.getUpdateData(key);
        this.positions[update.index] = update.value;
        callbackOnUpdateApplied(update.index, update.value);
        this.logger.setCompleted(key);
    }

    public void sendWriteResult(CSUpdateKey key) {
        CSUpdateData update = this.logger.getUpdateData(key);

        if (this.logger.containsClientData(key)) {
            var clientData = this.logger.getClientData(key);
            if (this.id == clientData.contactedReplicaId) {
                super.debug("Sending WriteResult for " + key + " to client " + clientData.actor + "UUID: " + clientData.askUUID);
                super.tell(new CSWriteResult(update.index, update.value, true, this.id, clientData.askUUID), clientData.actor);
            } else {
                super.debug("I'm not the original handler of update " + key + ", no need to send result to any client");
            }
        }
    }

    // TODO: implement coordinator crash detection
    // - Coordinator multicasts heartbeat messages
    // - Replicas monitor heartbeat
    // - Coordinator election is triggered if heartbeat is not received or if expected messages are not received
    private void handleHeartBeatCoordinator(CSHeartBeatFromCoordinator msg) {
        this.broadcast(new CSHeartBeatFromCoordinator(), Optional.empty());

        super.debug("Coordinator HeartBeat");
    }

    private void handleHeartBeatFromCoordinator(CSHeartBeatFromCoordinator msg) {
        if (this.heartBeatTimer == null) {
            // This is the first heartbeat from the coordinator
            // as it is up and running correctly we can start checking the heartbeat
            this.heartBeatTimer = getContext().system().scheduler().scheduleWithFixedDelay(
                    Duration.ofMillis(0),
                    Duration.ofMillis(AbstractReplica.COORDINATOR_BEAT_INTERVAL + super.getMaxLatencyPlusTolerance()),
                    getSelf(),
                    new CSHeartBeatCheck(),
                    getContext().system().dispatcher(),
                    ActorRef.noSender());
        }
        this.heartBeatReceived = true;

        super.debug("Received Coordinator HeartBeat");
    }

    private void handleHeartBeatCheck(CSHeartBeatCheck msg) {
        if (this.heartBeatReceived) {
            this.heartBeatReceived = false;
        } else {
            this.election();
        }
    }

    // TODO: implement coordinator election
    // - When crash detected: send ELECTION message to next replica
    // - Each replica adds its knowledge of the latest update and forwards
    // - If timeout: skip crashed replica and forward to next
    // - When the ring is completed, forward again to the new coordinator (Replica with most recent update wins, break ties with replica ID)
    // - New coordinator sends SYNCHRONIZATION and replicas update their coordinator reference
    // - Sends any missing updates to other replicas
    public final void election() {
        if (this.heartBeatTimer != null) {
            this.heartBeatTimer.cancel();
            this.heartBeatTimer = null;
        }

        super.debug("Election Started");
    }

    private void becomeCoordinator() {
        getContext().become(coordinator());
        this.heartBeatTimer = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.ofMillis(0),
                Duration.ofMillis(AbstractReplica.COORDINATOR_BEAT_INTERVAL),
                getSelf(),
                new CSHeartBeatFromCoordinator(),
                getContext().system().dispatcher(),
                ActorRef.noSender());
    }

    @FunctionalInterface
    public interface ReplicaHandler {
        void handle(Integer replica_id, CSAskMessage result, boolean timedOut);
    }

    /**
     * send to every replica but itself
     *
     * @param msg
     * @param timeout
     * @param crash_message_n
     * @param handler
     */
    public final void broadcast(CSAskMessage msg, Duration timeout, Optional<Integer> crash_message_n,
                                ReplicaHandler handler
    ) {
        var group = new HashMap<>(this.replicas);
        group.remove(this.id);

        int i = 0;
        for (var replica : group.entrySet()) {
            if (crash_message_n.isPresent() && i == crash_message_n.get()) {
                // crash
            }

            askSystem.ask(msg, replica.getValue(), timeout, (res, timedOut) -> {
                handler.handle(replica.getKey(), res, timedOut);
            });

            i++;
        }
    }

    /**
     * send to every replica but itself
     *
     * @param msg
     * @param crash_message_n
     */
    public final void broadcast(Serializable msg, Optional<Integer> crash_message_n) {
        var group = new HashMap<>(this.replicas);
        group.remove(this.id); // multicast does not include coordinator

        int i = 0;
        for (var replica : group.entrySet()) {
            if (crash_message_n.isPresent() && i == crash_message_n.get()) {
                // crash
            }
            super.tell(msg, replica.getValue());
        }
    }

    public final void multicast(HashMap<Integer, ActorRef> group, Supplier<CSAskMessage> msgFactory, Duration timeout,
                                Optional<Integer> crash_message_n,
                                ReplicaHandler handler
    ) {
        int i = 0;
        for (var replica : group.entrySet()) {
            if (crash_message_n.isPresent() && i == crash_message_n.get()) {
                // crash
            }

            askSystem.ask(msgFactory.get(), replica.getValue(), timeout, (res, timedOut) -> {
                handler.handle(replica.getKey(), res, timedOut);
            });

            i++;
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
                .match(CSHeartBeatFromCoordinator.class, this::handleHeartBeatFromCoordinator)
                .match(CSHeartBeatCheck.class, this::handleHeartBeatCheck)

                // ask handlers
                .match(CSAskTimeout.class, askSystem::handleTimeout)
                .build();
    }

    public final Receive coordinator() {
        return receiveBuilder()
                .match(CSReadRequest.class, this::handleReadRequest)
                .match(CSWriteRequest.class, this::handleWriteRequestCoordinator)
                .match(CSWriteForward.class, this::handleWriteForward)
                .match(CSHeartBeatFromCoordinator.class, this::handleHeartBeatCoordinator)

                // ask handlers
                .match(CSAck.class, askSystem::handleResponse)
                .match(CSAskTimeout.class, askSystem::handleTimeout)
                .build();
    }

    private Receive crashed() {
        return receiveBuilder().build();
    }
}
