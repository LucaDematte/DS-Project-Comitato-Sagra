package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds.cs.*;
import it.unitn.ds.cs.messages.AskRequest;
import it.unitn.ds.cs.messages.AskResponse;
import it.unitn.ds.cs.messages.AskTimeout;
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
    private final AskResponseSystem askSupport = new AskResponseSystem(getContext(), this::tell);
    
    // Replica
    Map<Integer, ActorRef> replicas = new HashMap<>(AbstractReplica.POSITIONS_LIST_LENGTH);
    int previous, next;
    int coordinatorId;
    int[] positions; //maybe do a hashmap
    UpdateLog updateLog;
    
    // Coordinator
    CSUpdateKey nextUpdateKey;
    Map<CSUpdateKey, Integer> receivedAcks = new HashMap<>();
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
        this.nextUpdateKey = new CSUpdateKey(0, 0);
        this.processing = false;
        this.updateLog = new UpdateLog();
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
    
    // Handler for all requests (coming from an ask) that need to be answered with a response
    private void onAskRequest(AskRequest request) {
        Serializable payload = request.getPayload();
        
        if (payload instanceof CSReadRequest) {
            // handleReadRequest
            CSReadRequest msg = (CSReadRequest) payload;
            
            try {
                int result = this.positions[msg.index];
                tell(AskResponseSystem.reply(request,
                        new CSReadResult(true, msg.index, result, this.id)), getSender());
            } catch (IndexOutOfBoundsException e) {
                tell(AskResponseSystem.reply(request,
                        new CSReadResult(false, msg.index, 0, this.id)), getSender());
            }
        } else if (payload instanceof CSWriteRequest) {
            // handleWriteRequest
            CSWriteRequest msg = (CSWriteRequest) payload;
            
            if (this.id == this.coordinatorId) {
                //        log("Write Request Received from" + this.id);
                
                UUID uuid = this.updateLog.addLocal(request.getCorrelationId(), msg.index,
                        msg.value, UpdateStatus.UNPROCESSED, getSender());
                log("Adding new local update coming from client " + getSender().path()
                                                                               .name() + " with uuid " + uuid);
                log("Adding update to queue");
                this.queue.add(new CSWriteForward(msg, uuid));
                processNextUpdate();
            } else {
                UUID uuid = this.updateLog.addLocal(request.getCorrelationId(), msg.index,
                        msg.value, UpdateStatus.UNPROCESSED, getSender());
                log("Adding new local update coming from client " + getSender().path()
                                                                               .name() + " with uuid " + uuid);
                
                Duration timeout = Duration.ofMillis(AbstractReplica.MAX_LATENCY * 2);
                
                askSupport.<CSAck>ask(new CSWriteForward(msg, uuid),
                        replicas.get(this.coordinatorId), timeout, (res, timedOut) -> {
                            if (timedOut) {
                                election();
                            }
                        });
            }
        } else if (payload instanceof CSWriteForward) {
            // handleWriteForward
            CSWriteForward msg = (CSWriteForward) payload;
            
            log("Adding update to queue");
            this.queue.add(msg);
            tell(AskResponseSystem.reply(request, new CSAck()), getSender());
            processNextUpdate();
        } else if (payload instanceof CSUpdate) {
            // handleUpdate
            CSUpdate msg = (CSUpdate) payload;
            
            log("Received Update with key:" + msg.key());
            this.updateLog.addRemote(msg.key(), msg.uuid(), msg.update().index(),
                    msg.update().value());
            tell(AskResponseSystem.reply(request, new CSAck()), getSender());
        }
    }
    
    public void handleReadRequest(CSWriteRequest msg) {
        try {
            int result = this.positions[msg.index];
            tell(AskResponseSystem.reply(request,
                    new CSReadResult(true, msg.index, result, this.id)), getSender());
        } catch (IndexOutOfBoundsException e) {
            tell(AskResponseSystem.reply(request, new CSReadResult(false, msg.index, 0, this.id)),
                    getSender());
        }
    }
    
    public void handleWriteRequest(CSWriteRequest msg) {
        UUID uuid = this.updateLog.addLocal(request.getCorrelationId(), msg.index, msg.value,
                UpdateStatus.UNPROCESSED, getSender());
        log("Adding new local update coming from client " + getSender().path()
                                                                       .name() + " with uuid " + uuid);
        
        Duration timeout = Duration.ofMillis(AbstractReplica.MAX_LATENCY * 2);
        
        askSupport.<CSAck>ask(new CSWriteForward(msg, uuid), replicas.get(this.coordinatorId),
                timeout, (res, timedOut) -> {
                    if (timedOut) {
                        election();
                    }
                });
    }
    
    public void handleWriteRequestCoordinator(CSWriteRequest msg) {
        //        log("Write Request Received from" + this.id);
        
        UUID uuid = this.updateLog.addLocal(request.getCorrelationId(), msg.index, msg.value,
                UpdateStatus.UNPROCESSED, getSender());
        log("Adding new local update coming from client " + getSender().path()
                                                                       .name() + " with uuid " + uuid);
        log("Adding update to queue");
        this.queue.add(new CSWriteForward(msg, uuid));
        processNextUpdate();
    }
    
    public void handleWriteForward(CSWriteForward msg) {
    
    }
    
    // =================================================================================
    // Update Protocol
    // =================================================================================
    
    public void processNextUpdate() {
        if (!this.processing) {
            if (!this.queue.isEmpty()) {
                this.processing = true;
                CSWriteForward current = this.queue.poll();
                log("Starting processing a message");
                update(current);
            } else {
                log("No more messages to process");
            }
        } else {
            log("Already processing a message");
        }
    }
    
    public final void update(CSWriteForward msg) {
        // Deep copy to avoid working on actor state
        CSUpdateKey updateKey = new CSUpdateKey(this.nextUpdateKey);
        
        this.updateLog.addRemote(updateKey, msg.uuid(), msg.request().index, msg.request().value);
        this.receivedAcks.put(updateKey, 1); // coordinator immediately acks to itself
        
        // TODO Now that we use the right tell() (with network delays) we must choose timeouts wisely
        Duration timeout = Duration.ofMillis(1000);
        multicast(new CSUpdate(updateKey,
                        new CSUpdateValue(msg.request().index, msg.request().value, false), msg.uuid()),
                timeout, Optional.empty(), (replicaId, res, timedOut) -> {
                    if (!timedOut) {
                        log("Received ack from: " + replicaId);
                        this.receivedAcks.put(updateKey, this.receivedAcks.get(updateKey) + 1);
                        
                        // slash with integers always floor, happens only once because of ==
                        if (this.receivedAcks.get(updateKey) == this.replicas.size() / 2 + 1) {
                            log("Quorum reached for update " + updateKey + ", sending WriteOk to replicas");
                            multicast(new CSWriteOk(updateKey), Optional.empty());
                            
                            completeUpdate(updateKey);
                            
                            this.processing = false;
                            processNextUpdate();
                        }
                    } else {
                        int crashed_replica_id = replicaId;
                        
                        log("Replica with ID " + crashed_replica_id + " crashed! Sending notices to replicas");
                        
                        // TODO removing replicas from the list causes problems with the quorum calculation
                        // Also, it is not possible since it is an unmodifiable map
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
        this.nextUpdateKey = new CSUpdateKey(this.nextUpdateKey.epoch(),
                this.nextUpdateKey.seq_no() + 1); //TODO check if final is needed
    }
    
    public final void handleWriteOk(CSWriteOk msg) {
        log("Received WriteOk with key:" + msg.key);
        
        //      try {
        completeUpdate(msg.key);
        //      }catch(Exception e){
        //          log("Error while handling writeOk: " + e.getMessage());
        //      }
        
    }
    
    public void completeUpdate(CSUpdateKey key) {
        //log("Writing to positions");
        UpdateData update = this.updateLog.get(key);
        this.positions[update.index] = update.value;
        callbackOnUpdateApplied(update.index, update.value);
        this.updateLog.setCompleted(key);
        sendWriteResult(key);
    }
    
    public void sendWriteResult(CSUpdateKey key) {
        UpdateData update = this.updateLog.get(key);
        UUID updateUUID = this.updateLog.getUUIDBinding(key);
        if (update.localRequest) {
            log("Sending WriteResult to client " + update.client.path().name());
            tell(AskResponseSystem.reply(new AskRequest(updateUUID, null),
                    new CSWriteResult(true, update.index, update.value, this.id)), update.client);
        } else {
            //log("I'm not the original handler of this update, no need to send result to any client");
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
    
    public final void multicast(
            Serializable msg, Duration timeout, Optional<Integer> crash_message_n,
            ReplicaHandler handler
    ) {
        var group = new HashMap<>(this.replicas);
        group.remove(this.coordinatorId); // multicast does not include coordinator
        
        int i = 0;
        for (var replica : group.entrySet()) {
            if (crash_message_n.isPresent() && i == crash_message_n.get()) {
                // crash
            }
            
            askSupport.ask(msg, replica.getValue(), timeout, (res, timedOut) -> {
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
        return createBaseReceiveBuilder().match(CSCrashNotice.class, this::handleCrashNotice)
                                         .match(CSWriteOk.class, this::handleWriteOk)
                                         // handlers for messages in the ask-response system
                                         .match(AskResponse.class, askSupport::handleResponse)
                                         .match(AskRequest.class, this::onAskRequest)
                                         .match(AskTimeout.class, askSupport::handleTimeout)
                                         .build();
    }
    
    public final Receive coordinator() {
        return receiveBuilder()
                // handlers for messages in the ask-response system
                .match(AskResponse.class, askSupport::handleResponse)
                .match(AskRequest.class, this::onAskRequest)
                .match(AskTimeout.class, askSupport::handleTimeout)
//              .match(CSUpdate.class, this::handleUpdate)
//              .match(CSWriteOk.class, this::handleWriteOk)
                .build();
    }
    
}
