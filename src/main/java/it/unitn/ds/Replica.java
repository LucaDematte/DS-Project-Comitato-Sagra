package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.pattern.Patterns;
import it.unitn.ds.cs.*;
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
    int[] positions; //maybe do a hashmap
    UpdateLog updateLog;
    
    // Coordinator
    CSUpdateKey nextUpdateKey;
    Map<CSUpdateKey, Integer> receivedAcks = new HashMap<>();
    Queue<CSWriteForward> queue = new LinkedList<>();
    boolean processing;
    
    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
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
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }
    
    // Props method for automated tests
    public static Props propsWithListener(
            int id, int minLatency, int maxLatency, int coordinatorBeatInterval,
            ActorRef listener
    ) {
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
    
    // =================================================================================
    // Read Requests
    // =================================================================================
    
    public void handleReadRequest(CSReadRequest msg) {
        try {
            int result = this.positions[msg.index];
            getSender().tell(new CSReadResult(true, msg.index, result, this.id), getSelf());
        } catch (IndexOutOfBoundsException e) {
            getSender().tell(new CSReadResult(false, msg.index, 0, this.id), getSelf());
        }
    }
    
    // =================================================================================
    // Write Requests
    // =================================================================================
    
    public void handleWriteRequest(CSWriteRequest msg) {
        UUID uuid = this.updateLog.addLocal(msg.index, msg.value, UpdateStatus.UNPROCESSED, getSender());
        log("Adding new local update coming from client " + getSender().path()
                                                                       .name() + " with uuid " + uuid);
        
        Duration timeout = Duration.ofMillis(AbstractReplica.MAX_LATENCY * 2);
        CompletionStage<Object> future = Patterns.ask(replicas.get(this.coordinatorId), new CSWriteForward(msg, uuid), timeout);
        
        future.exceptionally(e -> {
            election();
            
            return null;
        });
    }
    
    public void handleWriteRequestCoordinator(CSWriteRequest msg) {
        //        log("Write Request Received from" + this.id);
        
        UUID uuid = this.updateLog.addLocal(msg.index, msg.value, UpdateStatus.UNPROCESSED, getSender());
        log("Adding new local update coming from client " + getSender().path()
                                                                       .name() + " with uuid " + uuid);
        log("Adding update to queue");
        this.queue.add(new CSWriteForward(msg, uuid));
        //processUpdates();
        processNextUpdate();
    }
    
    public void handleWriteForward(CSWriteForward msg) {
        log("Adding update to queue");
        this.queue.add(msg);
        getSender().tell(new CSAck(), getSelf());
        //processUpdates();
        processNextUpdate();
    }
    
    // =================================================================================
    // Update Protocol
    // =================================================================================
    
    //    public void processUpdates(){
    //        if (!this.processing) {
    //            log("Starting processing the queue");
    //            this.processing = true;
    //
    //
    //            log("Processing queue");
    //            while (!this.queue.isEmpty()) {
    //                CSWriteForward current = this.queue.poll();
    //                update(current);
    //            }
    //
    //            log("Ending processing the queue");
    //            this.processing = false;
    //        }else{
    //            log("Already processing the queue");
    //        }
    //    }
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
        CSUpdateKey updateKey = new CSUpdateKey(this.nextUpdateKey);
        
        this.updateLog.addRemote(updateKey, msg.uuid(), msg.request().index, msg.request().value);
        this.receivedAcks.put(updateKey, 1); // coordinator immediately acks to itself
        
        Duration timeout = Duration.ofMillis(AbstractReplica.MAX_LATENCY * 2);
        multicast(new CSUpdate(updateKey, new CSUpdateValue(msg.request().index, msg.request().value, false), msg.uuid()), timeout, Optional.empty(), (replicaId, res, e) -> {
            if (e == null) {
                getSelf().tell(new CSReplicaAck(replicaId, updateKey), getSelf());
            } else {
                getSelf().tell(new CSReplicaCrash(replicaId), getSelf());
            }
        });
        this.nextUpdateKey = new CSUpdateKey(this.nextUpdateKey.epoch(), this.nextUpdateKey.seq_no() + 1); //TODO check if final is
        // needed
    }
    
    public final void handleUpdate(CSUpdate msg) {
        log("Received Update with key:" + msg.key());
        this.updateLog.addRemote(msg.key(), msg.uuid(), msg.update().index(), msg.update().value());
        getSender().tell(new CSAck(), getSelf());
        
    }
    
    public final void handleReplicaAck(CSReplicaAck msg) {
        log("Received ack from: " + msg.replicaId);
        this.receivedAcks.put(msg.updateKey, this.receivedAcks.get(msg.updateKey) + 1);
        if (this.receivedAcks.get(msg.updateKey) == this.replicas.size() / 2 + 1) { // slash with integers always floor
            // happens only once because of ==
            //this.positions[msg.request().index] = msg.request().value;
            log("Quorum reached for update " + msg.updateKey + ", sending WriteOk to replicas");
            multicast(new CSWriteOk(msg.updateKey), Optional.empty());
            
            completeUpdate(msg.updateKey);
            
            this.processing = false;
            processNextUpdate();
        }
    }
    
    public record CSReplicaAck(int replicaId, CSUpdateKey updateKey) implements Serializable {}
    
    public final void handleReplicaCrash(CSReplicaCrash msg) {
        int crashed_replica_id = msg.replicaId;
        this.replicas.remove(crashed_replica_id);   // TODO removing replicas from the list causes problems with the quorum calculation
        
        int previous_replica_id = this.replicas.keySet()
                                               .stream()
                                               .filter(id -> id < crashed_replica_id)
                                               .max(Integer::compare)
                                               .orElse(Collections.min(this.replicas.keySet()));
        int next_replica_id = this.replicas.keySet()
                                           .stream()
                                           .filter(id -> id > crashed_replica_id)
                                           .min(Integer::compare)
                                           .orElse(Collections.max(this.replicas.keySet()));
        this.replicas.get(previous_replica_id)
                     .tell(new CSCrashNotice(crashed_replica_id, previous_replica_id, next_replica_id), getSelf());
        this.replicas.get(next_replica_id)
                     .tell(new CSCrashNotice(crashed_replica_id, previous_replica_id, next_replica_id), getSelf());
    }
    
    public record CSReplicaCrash(int replicaId) implements Serializable {}
    
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
        if (update.localRequest) {
            log("Sending WriteResult to client " + update.client.path().name());
            update.client.tell(new CSWriteResult(true, update.index, update.value, this.id), getSelf());
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
        void handle(Integer replica_id, Object result, Throwable error);
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
        return createBaseReceiveBuilder().match(CSReadRequest.class, this::handleReadRequest)
                                         .match(CSWriteRequest.class, this::handleWriteRequest)
                                         .match(CSCrashNotice.class, this::handleCrashNotice)
                                         .match(CSUpdate.class, this::handleUpdate)
                                         .match(CSWriteOk.class, this::handleWriteOk)
                                         .build();
    }
    
    public final Receive coordinator() {
        return receiveBuilder().match(CSReadRequest.class, this::handleReadRequest)
                               .match(CSWriteRequest.class, this::handleWriteRequestCoordinator)
                               .match(CSWriteForward.class, this::handleWriteForward)
                               // Internal messages sent by coordinator to itself
                               .match(CSReplicaAck.class, this::handleReplicaAck)
                               .match(CSReplicaCrash.class, this::handleReplicaCrash)
                               //                             .match(CSUpdate.class, this::handleUpdate)
                               //                             .match(CSWriteOk.class, this::handleWriteOk)
                               .build();
    }
    
}
