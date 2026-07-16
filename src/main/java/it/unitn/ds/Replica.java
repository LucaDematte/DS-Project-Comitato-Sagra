package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds.cs.CSAsk;
import it.unitn.ds.cs.CSCrashSystem;
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
    /**
     * System to send messages (with ask) that expect a response within a given timeout.
     * More info at {@link CSAsk}.
     */
    CSAsk askSystem = new CSAsk(getContext(), super::tell);
    /**
     * Timer for the periodic operation of heartbeat sending and reception.
     * The coordinator uses this timer to send a message to itself every second to remember to send
     * heartbeat to other replicas.
     * A normal replica uses this timer to check every second if it has received
     * {@link CSHeartBeatFromCoordinator} or not.
     */
    Cancellable heartBeatTimer;
    /**
     * System that maintains information on how and when this replica should crash.
     * More info at {@link CSCrashSystem}.
     */
    CSCrashSystem crashSystem = new CSCrashSystem();
    
    // ===================================== REPLICA =====================================
    
    /**
     * Maps replica IDs to their reference ({@link ActorRef}).
     * NOTE: crashed replicas are removed from this map.
     */
    Map<Integer, ActorRef> replicas = new HashMap<>(AbstractReplica.POSITIONS_LIST_LENGTH);
    /** ID of the previous and next replica in the ring topology used during election. */
    int previous, next;
    /** ID of the replica currently working as coordinator. */
    int coordinatorId;
    /** Array of positions of the secret agents (shared data among replicas). */
    int[] positions; //maybe do a hashmap
    /** System to log update information coming from clients or the coordinator. */
    final CSLogger logger = new CSLogger();
    /**
     * Flag that signals if a heartbeat message has been received in the time frame of the last
     * second.
     * The flag must be set to true whenever a new heartbeat message is received.
     * The flag must be set to {@code false} when the replica checks its value.
     * If the flag is already {@code false}, this means that the coordinator didn't send a new
     * heartbeat after the previous check.
     */
    boolean heartBeatReceived;
    
    // =================================== COORDINATOR ===================================
    
    /** The next update key to be assigned at a new update. */
    CSUpdateKey updateKey;
    /**
     * Maps update keys to the number of ACKs received.
     * The coordinator must increment the value in this map whenever an ACK for an update is
     * received.
     */
    Map<CSUpdateKey, Integer> receivedAcks = new HashMap<>();
    /**
     * This queue stores the update operations that still need to be processed by the coordinator.
     * The queue is of {@link CSWriteForward} instead of {@link CSWriteRequest} because additional
     * information is needed and are inserted in {@link CSWriteForward} to leave the implementation
     * of {@link CSWriteRequest} clean.
     */
    Queue<CSWriteForward> queue = new LinkedList<>();
    /**
     * Flag set to {@code true} while the coordinator is processing an update.
     * Used to prevent concurrent processing of multiple updates.
     */
    boolean processing;
    
    // =================================================================================
    // Builder methods & initialization
    // =================================================================================
    
    public Replica(int id) {
        this(id,
             AbstractReplica.MIN_LATENCY,
             AbstractReplica.MAX_LATENCY,
             AbstractReplica.COORDINATOR_BEAT_INTERVAL,
             Optional.empty()
        );
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
                            () -> new Replica(id,
                                              minLatency,
                                              maxLatency,
                                              coordinatorBeatInterval,
                                              Optional.empty()
                            )
        );
    }
    
    // Props method for automated tests
    public static Props propsWithListener(
            int id, int minLatency, int maxLatency,
            int coordinatorBeatInterval, ActorRef listener
    ) {
        return Props.create(Replica.class,
                            () -> new Replica(id,
                                              minLatency,
                                              maxLatency,
                                              coordinatorBeatInterval,
                                              Optional.ofNullable(listener)
                            )
        );
    }
    
    @Override
    public int getSystemNumberOfActors() {
        return this.replicas.size();
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
            // The timer for replicas is initialized after the first heartbeat is received
            this.heartBeatTimer = null;
        }
    }
    
    // =================================================================================
    // READ REQUESTS
    // =================================================================================
    
    /**
     * Handler for read requests coming from clients.
     * A read request is resolved by immediately returning the value stored in this replica.
     * This handler is used by normal replicas and coordinator.
     *
     * @param msg The request message sent by the client.
     */
    private void handleReadRequest(CSReadRequest msg) {
        try {
            int result = this.positions[msg.index];
            tell(new CSReadResult(true, msg.index, result, this.id, msg.askUUID), getSender());
        } catch (IndexOutOfBoundsException e) {
            tell(new CSReadResult(false, msg.index, 0, this.id, msg.askUUID), getSender());
        }
    }
    
    // =================================================================================
    // WRITE REQUESTS
    // =================================================================================
    
    /**
     * Handler for write requests coming from clients.
     * A write request is resolved by forwarding it to the coordinator, which will use the update
     * protocol so that the update is applied to all replicas.
     * This handler is used only by normal replicas.
     *
     * @param msg The write request sent by the client.
     */
    private void handleWriteRequest(CSWriteRequest msg) {
        // To let this replica know which update key will be assigned by the coordinator, a UUID is
        // generated and appended to the request. The coordinator will send the UPDATE message
        // containing both the update key and the UUID.
        UUID writeRequestUUID = UUID.randomUUID();
        
        // The update is logged in the local update list of the replica
        var clientData = new CSClientData(getSender(), this.id, msg.askUUID);
        this.logger.logRequest(writeRequestUUID,
                               new CSUpdateData(msg.index, msg.value, false),
                               clientData
        );
        
        super.debug("Adding new update coming from client " + getSender().path()
                                                                         .name() + " with uuid " + writeRequestUUID);
        
        // The request is forwarded to the coordinator
        Duration timeout = Duration.ofMillis(super.getMaxLatencyPlusTolerance());
        askSystem.<CSUpdate>ask(new CSWriteForward(msg, writeRequestUUID, clientData),
                                replicas.get(this.coordinatorId),
                                timeout,
                                (res, timedOut) -> {
                                    if (timedOut) {
                                        // If the UPDATE message is not received, the coordinator must have crashed
                                        election();
                                    } else {
                                        super.debug("Forward did not time out");
                                    }
                                }
        );
    }
    
    /**
     * Special handler for write requests sent directly to the coordinator.
     * The write request is added to the queue and then processed with the update protocol so that
     * the update is applied to all replicas.
     * This handler is used only by the coordinator.
     *
     * @param msg The write request sent by the client.
     */
    private void handleWriteRequestCoordinator(CSWriteRequest msg) {
        // To let this replica know which update key will be assigned by the coordinator, a UUID is
        // generated and appended to the request. The coordinator will send the UPDATE message
        // containing both the update key and the UUID.
        UUID writeRequestUUID = UUID.randomUUID();
        super.debug("WriteRequest askUUID: " + msg.askUUID);
        
        // The update is logged in the local update list of the coordinator
        var clientData = new CSClientData(getSender(), this.id, msg.askUUID);
        this.logger.logRequest(writeRequestUUID,
                               new CSUpdateData(msg.index, msg.value, false),
                               clientData
        );
        
        super.debug("Received write request from client: " + getSender().path()
                                                                        .name() + " - uuid: " + writeRequestUUID);
        
        // The request is added to the queue
        this.queue.add(new CSWriteForward(msg, writeRequestUUID, clientData));
        super.debug("Update added to queue");
        
        // Since the queue must have at least one update waiting to be processed, the routine to process updates is called
        processUpdates();
    }
    
    /**
     * Handler to receive requests forwarded to the coordinator.
     * The update request is added to the queue of the coordinator.
     * This handler is used only by the coordinator.
     *
     * @param msg The write request forwarded by a replica to the coordinator.
     */
    private void handleWriteForward(CSWriteForward msg) {
        super.debug("Adding update to queue");
        // The request is added to the queue
        this.queue.add(msg);
        // Since the queue must have at least one update waiting to be processed, the routine to process updates is called
        processUpdates();
    }
    
    // =================================================================================
    // Update Protocol
    // =================================================================================
    
    /**
     * This method must be called whenever there are updates waiting to be processed in the queue of
     * the coordinator.
     * It ensures that only one update at a time is being processed.
     */
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
    
    /**
     * This method processes one update by following the specification of the update protocol.
     * A new UPDATE message ({@link CSUpdate}) with an update key is sent to all replicas so that
     * they get to know the update data.
     * Inside this method it is also defined the behavior of the coordinator when it receives an ACK
     * for the UPDATE message (inside a lambda).
     *
     * @param msg The data of the update to be processed.
     */
    private void update(CSWriteForward msg) {
        // Deep copy to avoid working on actor state
        CSUpdateKey updateKey = new CSUpdateKey(this.updateKey);
        
        // The coordinator logs the update in its local update list (if he didn't do it already before in handleWriteRequestCoordinator)
        CSUpdateData data = new CSUpdateData(msg.request.index, msg.request.value, false);
        this.logger.logUpdate(updateKey, msg.writeRequestUUID, data);
        
        // The number of received ACKs for this update is initialized
        this.receivedAcks.put(updateKey, 1); // coordinator immediately acks to itself
        
        // This map contains all the replicas that don't already know the existence of this update
        var otherReplicas = new HashMap<>(this.replicas);
        otherReplicas.remove(this.coordinatorId); // same as this.id because update is executed only by the coordinator
        otherReplicas.remove(msg.clientData.contactedReplicaId);
        
        Duration timeout = Duration.ofMillis(2L * super.getMaxLatencyPlusTolerance());
        
        // This lambda contains the behavior of the coordinator when an ACK for this update is received (or when the timeout runs out)
        ReplicaHandler handler = (replicaId, res, timedOut) -> {
            if (!timedOut) {
                super.debug("Received ack from: " + replicaId);
                this.receivedAcks.put(updateKey, this.receivedAcks.get(updateKey) + 1);
                
                // slash with integers always floor
                if (this.receivedAcks.get(updateKey) == this.replicas.size() / 2 + 1) { // happens only once because of ==
                    super.debug("Quorum reached for update " + updateKey + ", sending WriteOk to replicas");
                    broadcast(new CSWriteOk(updateKey));
                    
                    this.completeUpdate(updateKey);
                    this.sendWriteResult(updateKey);
                    this.processing = false;
                    processUpdates(); // continue processing other updates until the queue is empty
                }
            } else {
                int crashed_replica_id = replicaId;
                
                super.debug("Replica with ID " + crashed_replica_id + " crashed! Sending notices to replicas");
                
                this.replicas.remove(crashed_replica_id);
                
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
                             .tell(new CSCrashNotice(crashed_replica_id,
                                                     previous_replica_id,
                                                     next_replica_id
                                   ), getSelf()
                             );
                this.replicas.get(next_replica_id)
                             .tell(new CSCrashNotice(crashed_replica_id,
                                                     previous_replica_id,
                                                     next_replica_id
                                   ), getSelf()
                             );
            }
        };
        
        // The coordinator sends the UPDATE message to the replica which forwarded the request
        // note: coordinator never sends messages to itself
        // always handles updates by managing internal state
        if (this.coordinatorId != msg.clientData.contactedReplicaId) {
            askSystem.<CSAck>ask(new CSUpdate(updateKey, data, msg.writeRequestUUID, msg.askUUID),
                                 this.replicas.get(msg.clientData.contactedReplicaId),
                                 timeout,
                                 (res, timedOut) -> {
                                     handler.handle(msg.clientData.contactedReplicaId,
                                                    res,
                                                    timedOut
                                     );
                                 }
            );
            
            if (this.crashSystem.shouldCrashAfterThisUpdate()) {
                this.becomeCrashed();
            }
        }
        
        // The coordinator sends the UPDATE message to all other replicas
        this.multicast(otherReplicas,
                       () -> new CSUpdate(updateKey, data, msg.writeRequestUUID),
                       timeout,
                       handler
        );
        super.debug("Sent update" + updateKey + " to replicas");
        
        // The key to be assigned to the next update is saved
        this.updateKey = new CSUpdateKey(this.updateKey.epoch, this.updateKey.seq_no + 1);
    }
    
    /**
     * Handler to receive UPDATE messages ({@link CSUpdate}) sent by the coordinator.
     * This handler is used only by normal replicas.
     *
     * @param msg The update message coming from the coordinator.
     */
    private void handleUpdate(CSUpdate msg) {
        super.debug("Received Update with key:" + msg.key);
        // The replica logs the update in its local update list and sends an ACK back to the coordinator
        this.logger.logUpdate(msg.key, msg.writeRequestUUID, msg.data);
        super.tell(new CSAck(msg.askUUID), getSender());
    }
    
    /**
     * Handler to receive WRITEOK messages ({@link CSWriteOk}) sent by the coordinator.
     * This handler is used only by normal replicas.
     *
     * @param msg The message coming from the coordinator.
     */
    public final void handleWriteOk(CSWriteOk msg) {
        super.debug("Received WriteOk with key:" + msg.key);
        
        this.completeUpdate(msg.key);
        this.sendWriteResult(msg.key);
    }
    
    /**
     * This method writes an update to the {@code positions} array and uses the callback of the
     * codebase.
     * This operation is done by normal replicas and coordinator.
     *
     * @param key The update key of the update to be completed.
     */
    public void completeUpdate(CSUpdateKey key) {
        super.debug("Writing to positions");
        CSUpdateData update = this.logger.getUpdateData(key);
        this.positions[update.index] = update.value;
        callbackOnUpdateApplied(update.index, update.value);
        this.logger.setCompleted(key);
    }
    
    /**
     * This method checks if the replica should be the one in charge of sending a write result back
     * to the client.
     * If this is the case, a write result ({@link CSWriteResult}) is sent to the client with the
     * correct {@code askUUID} to mark it as a response to the client ask.
     *
     * @param key The update key of the update.
     */
    public void sendWriteResult(CSUpdateKey key) {
        CSUpdateData update = this.logger.getUpdateData(key);
        
        if (this.logger.containsClientData(key)) {
            var clientData = this.logger.getClientData(key);
            if (this.id == clientData.contactedReplicaId) {
                super.debug("Sending WriteResult for " + key + " to client " + clientData.actor + "UUID: " + clientData.askUUID);
                super.tell(new CSWriteResult(update.index,
                                             update.value,
                                             true,
                                             this.id,
                                             clientData.askUUID
                           ), clientData.actor
                );
            } else {
                super.debug("I'm not the original handler of update " + key + ", no need to send result to any client");
            }
        }
    }
    
    // =================================================================================
    // HEARTBEAT
    // =================================================================================
    
    /**
     * Handler used by the coordinator to remember to send the HEARTBEAT to normal replicas.
     * This handler is used only by the coordinator.
     *
     * @param msg A message scheduled to be sent to the coordinator every second.
     */
    private void handleHeartBeatCoordinator(CSHeartBeatFromCoordinator msg) {
        this.broadcast(new CSHeartBeatFromCoordinator());
        
        super.debug("Coordinator HeartBeat");
    }
    
    /**
     * Handler used by normal replicas to receive the HEARTBEAT
     * ({@link CSHeartBeatFromCoordinator})
     * from the coordinator.
     * In this method, the flag {@code heartBeatReceived} is set to true so that a following
     * heartbeat check is successful.
     * This handler is used only by normal replicas.
     *
     * @param msg The HEARTBEAT message sent by the coordinator.
     */
    private void handleHeartBeatFromCoordinator(CSHeartBeatFromCoordinator msg) {
        if (this.heartBeatTimer == null) {
            // This is the first heartbeat from the coordinator
            // as it is up and running correctly we can start checking the heartbeat
            this.heartBeatTimer = getContext().system()
                                              .scheduler()
                                              .scheduleWithFixedDelay(Duration.ofMillis(0),
                                                                      Duration.ofMillis(
                                                                              AbstractReplica.COORDINATOR_BEAT_INTERVAL + super.getMaxLatencyPlusTolerance()),
                                                                      getSelf(),
                                                                      new CSHeartBeatCheck(),
                                                                      getContext().system()
                                                                                  .dispatcher(),
                                                                      ActorRef.noSender()
                                              );
        }
        this.heartBeatReceived = true;
        
        super.debug("Received Coordinator HeartBeat");
        
        if (this.crashSystem.shouldCrashAfterThisHeartBeat()) {
            this.becomeCrashed();
        }
    }
    
    /**
     * Handler used by normal replicas to check that, in the last second of operation, a HEARTBEAT
     * message from the coordinator has been received.
     * This handler is used only by normal replicas.
     *
     * @param msg A message scheduled by the replica to itself every second.
     */
    private void handleHeartBeatCheck(CSHeartBeatCheck msg) {
        if (this.heartBeatReceived) {
            this.heartBeatReceived = false;
        } else {
            // If the flag is already false, no HEARTBEAT has been received between this check and the previous one
            // so the coordinator must have crashed
            this.election();
        }
    }
    
    // =================================================================================
    // CRASH
    // =================================================================================
    
    @Override
    public void crash(AbstractReplica.Crash how_to_crash) {
        super.debug("Received crash instructions: after " + how_to_crash.after_n_messages_of_type + " messages of type " + how_to_crash.type.name());
        // Registering new instruction in the crash system
        this.crashSystem.addInstruction(how_to_crash);
        // Check if it was an instruction of type Now (if it is, become crashed)
        if (this.crashSystem.shouldCrashNow()) {
            this.becomeCrashed();
        }
    }
    
    /**
     * Method to be fired to change the status of a replica to crashed.
     */
    private void becomeCrashed() {
        super.debug("Switching to crashed mode");
        if (this.heartBeatTimer != null) {
            this.heartBeatTimer.cancel();
            this.heartBeatTimer = null;
        }
        getContext().become(crashed());
    }
    
    /**
     * Method used inside multicast and broadcast to check if the replica should crash after sending
     * the message only to a part of the replicas.
     *
     * @param msg The message being sent in multicast or broadcast. If the message is of a class
     *            that is a condition for crash ({@link CSUpdate}, {@link CSWriteOk}), the crash
     *            system is checked.
     * @return Whether the replica should crash right after calling this method.
     */
    private boolean checkForCrashAfterSendingMsg(Serializable msg) {
        switch (msg) {
            case CSUpdate msg1 -> {
                return this.crashSystem.shouldCrashAfterThisUpdate();
            }
            case CSWriteOk msg1 -> {
                return this.crashSystem.shouldCrashAfterThisWriteOk();
            }
            default -> {
                return false;
            }
        }
    }
    
    /**
     * Handler used by normal replicas to update the {@code next} and {@code previous} references
     * for the ring topology used during election.
     * This handler is used only by normal replicas.
     *
     * @param msg A message sent by the coordinator that specifies the new previous and next
     *            replicas.
     */
    public final void handleCrashNotice(CSCrashNotice msg) {
        if (this.id < msg.crashed_id) {
            this.next = msg.next;
        } else {
            this.previous = msg.previous;
        }
    }
    
    // =================================================================================
    // ELECTION
    // =================================================================================
    
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
    
    /**
     * Method to be fired to change the status of a replica to coordinator.
     */
    private void becomeCoordinator() {
        super.debug("Becoming coordinator");
        getContext().become(coordinator());
        this.heartBeatTimer = getContext().system()
                                          .scheduler()
                                          .scheduleWithFixedDelay(Duration.ofMillis(0),
                                                                  Duration.ofMillis(AbstractReplica.COORDINATOR_BEAT_INTERVAL),
                                                                  getSelf(),
                                                                  new CSHeartBeatFromCoordinator(),
                                                                  getContext().system()
                                                                              .dispatcher(),
                                                                  ActorRef.noSender()
                                          );
    }
    
    // =================================================================================
    // Multicast & Broadcast
    // =================================================================================
    
    @FunctionalInterface
    public interface ReplicaHandler {
        void handle(Integer replica_id, CSAskMessage result, boolean timedOut);
    }
    
    /**
     * Sends an ask message to every replica excluding itself.
     *
     * @param msgFactory A factory to generate the same message once for each replica.
     * @param timeout    The timeout in which a response should be received.
     * @param handler    The callback to execute when the response is received or when the timeout
     *                   runs out.
     */
    public final void broadcast(
            Supplier<CSAskMessage> msgFactory, Duration timeout,
            ReplicaHandler handler
    ) {
        var group = new HashMap<>(this.replicas);
        group.remove(this.id);
        
        this.multicast(group, msgFactory, timeout, handler);
    }
    
    /**
     * Sends a normal message to every replica excluding itself.
     *
     * @param msg The message to be sent to replicas.
     */
    public final void broadcast(Serializable msg) {
        var group = new HashMap<>(this.replicas);
        group.remove(this.id);
        
        for (var replica : group.entrySet()) {
            super.tell(msg, replica.getValue());
            
            // At every iteration, check if the replica should crash after sending the message
            if (this.checkForCrashAfterSendingMsg(msg)) {
                this.becomeCrashed();
                return;
            }
        }
    }
    
    /**
     * Sends an ask message to the specified group of replicas.
     *
     * @param group      The group of replicas the message has to be sent to.
     * @param msgFactory A factory to generate the same message once for each replica.
     * @param timeout    The timeout in which a response should be received.
     * @param handler    The callback to execute when the response is received or when the timeout
     *                   runs out.
     */
    public final void multicast(
            HashMap<Integer, ActorRef> group, Supplier<CSAskMessage> msgFactory, Duration timeout,
            ReplicaHandler handler
    ) {
        for (var replica : group.entrySet()) {
            CSAskMessage msg = msgFactory.get();
            
            askSystem.ask(msg, replica.getValue(), timeout, (res, timedOut) -> {
                              handler.handle(replica.getKey(), res, timedOut);
                          }
            );
            
            // At every iteration, check if the replica should crash after sending the message
            if (this.checkForCrashAfterSendingMsg(msg)) {
                this.becomeCrashed();
                return;
            }
        }
    }
    
    // =================================================================================
    // Receive builders
    // =================================================================================
    
    /**
     * Creates and builds a Receive with the bindings for normal replicas.
     *
     * @return The Receive object.
     */
    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder().match(CSReadRequest.class, this::handleReadRequest)
                                         .match(CSWriteRequest.class, this::handleWriteRequest)
                                         .match(CSCrashNotice.class, this::handleCrashNotice)
                                         .match(CSUpdate.class, this::handleUpdate)
                                         .match(CSWriteOk.class, this::handleWriteOk)
                                         .match(CSHeartBeatFromCoordinator.class,
                                                this::handleHeartBeatFromCoordinator
                                         )
                                         .match(CSHeartBeatCheck.class, this::handleHeartBeatCheck)
                                         
                                         // ask handlers
                                         .match(CSAskTimeout.class, askSystem::handleTimeout)
                                         .build();
    }
    
    /**
     * Creates and builds a Receive with the bindings for the coordinator.
     *
     * @return The Receive object.
     */
    public final Receive coordinator() {
        return createBaseReceiveBuilder().match(CSReadRequest.class, this::handleReadRequest)
                                         .match(CSWriteRequest.class,
                                                this::handleWriteRequestCoordinator
                                         )
                                         .match(CSWriteForward.class, this::handleWriteForward)
                                         .match(CSHeartBeatFromCoordinator.class,
                                                this::handleHeartBeatCoordinator
                                         )
                                         
                                         // ask handlers
                                         .match(CSAck.class, askSystem::handleResponse)
                                         .match(CSAskTimeout.class, askSystem::handleTimeout)
                                         .build();
    }
    
    /**
     * Creates and builds a Receive with the bindings for a crashed replica.
     *
     * @return The Receive object.
     */
    private Receive crashed() {
        return receiveBuilder().match(CSReadRequest.class, this::messageBlackHole)
                               .match(CSWriteRequest.class, this::messageBlackHole)
                               .match(CSWriteForward.class, this::messageBlackHole)
                               .match(CSCrashNotice.class, this::messageBlackHole)
                               .match(CSUpdate.class, this::messageBlackHole)
                               .match(CSWriteOk.class, this::messageBlackHole)
                               .match(CSHeartBeatFromCoordinator.class, this::messageBlackHole)
                               .match(CSHeartBeatCheck.class, this::messageBlackHole)
                               
                               // ask handlers
                               .match(CSAck.class, this::messageBlackHole)
                               .match(CSAskTimeout.class, this::messageBlackHole)
                               .build();
    }
    
    /**
     * Handler that behaves like a black hole: can take any message and does nothing.
     * This is useful to prevent Akka from printing warnings for "dead letters" when a crashed
     * replica doesn't handle a message.
     * By applying this handler to all messages, the output in console is cleaner of unwanted
     * prints.
     *
     * @param msg A message that needs to be ignored.
     */
    private void messageBlackHole(Serializable msg) {
    }
}