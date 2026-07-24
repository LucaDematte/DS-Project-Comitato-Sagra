package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds.cs.CSAsk;
import it.unitn.ds.cs.CSCrashSystem;
import it.unitn.ds.cs.CSUpdateData;
import it.unitn.ds.cs.CSUpdateKey;
import it.unitn.ds.cs.logger.CSClientData;
import it.unitn.ds.cs.logger.CSUpdateLogger;
import it.unitn.ds.cs.messages.CSAskMessage;
import it.unitn.ds.cs.messages.CSAskTimeout;
import it.unitn.ds.cs.messages.client.CSHeartBeatCheck;
import it.unitn.ds.cs.messages.client.CSReadRequest;
import it.unitn.ds.cs.messages.client.CSWriteRequest;
import it.unitn.ds.cs.messages.coordinator.*;
import it.unitn.ds.cs.messages.replica.*;

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
    Map<Integer, ActorRef> replicas;
    /** ID of the next replica in the ring topology used during election. */
    int next;
    /** ID of the replica currently working as coordinator. */
    int coordinatorId;
    /** Array of positions of the secret agents (shared data among replicas). */
    int[] positions = new int[AbstractReplica.POSITIONS_LIST_LENGTH];//maybe do a hashmap
    /** System to log update information coming from clients or the coordinator. */
    final CSUpdateLogger updateLogger = new CSUpdateLogger();
    /**
     * Flag that signals if a heartbeat message has been received in the time frame of the last
     * second.
     * The flag must be set to true whenever a new heartbeat message is received.
     * The flag must be set to {@code false} when the replica checks its value.
     * If the flag is already {@code false}, this means that the coordinator didn't send a new
     * heartbeat after the previous check.
     */
    boolean heartBeatReceived;
    Duration defaultTimeout;
    Map<CSUpdateKey, UUID> updatesAskUUIDs = new HashMap<>();
    
    boolean electing = false;
    int electionInitiatorId = -1; // this way the first election received is considered
    
    // =================================== COORDINATOR ===================================
    
    /** The next update key to be assigned at a new update. */
    CSUpdateKey updateKey = new CSUpdateKey(0, 0);
    /**
     * Maps update keys to the number of ACKs received.
     * The coordinator must increment the value in this map whenever an ACK for an update is
     * received.
     */
    Map<CSUpdateKey, Integer> receivedAcks = new HashMap<>();
    
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
        
        this.next = (this.id + 1) % this.replicas.size();
        
        // getMaxLatencyPlusTolerance() needs replicas to be initiated
        this.defaultTimeout = Duration.ofMillis((long) super.getMaxLatencyPlusTolerance() * 4);
        
        if (this.id == this.coordinatorId) {
            this.becomeCoordinator();
        } else {
            this.becomeReplica();
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
        this.updateLogger.logRequest(writeRequestUUID,
                                     new CSUpdateData(msg.index, msg.value, false, clientData)
        );
        
        super.debug(
                "Received write request from client " + getSender().path().name() + " with uuid " +
                        writeRequestUUID.toString().substring(0, 8) +
                        ", forwarding to coordinator");
        
        askSystem.<CSUpdate>ask(new CSWriteForward(msg, writeRequestUUID, clientData),
                                replicas.get(this.coordinatorId),
                                this.defaultTimeout,
                                (res, timedOut) -> {
                                    if (timedOut) {
                                        // If the UPDATE message is not received, the coordinator must have crashed
                                        super.debug(
                                                "No update received after forwarding request with uuid " +
                                                        writeRequestUUID.toString()
                                                                        .substring(0, 8) +
                                                        ". Starting election.");
                                        startElection();
                                    } else {
                                        //super.debug("Forward did not time out");
                                    }
                                }
        );
    }
    
    private void handleWriteRequestDuringElection(CSWriteRequest msg) {
        // To let this replica know which update key will be assigned by the coordinator, a UUID is
        // generated and appended to the request. The coordinator will send the UPDATE message
        // containing both the update key and the UUID.
        UUID writeRequestUUID = UUID.randomUUID();
        
        // The update is logged in the local update list of the replica
        var clientData = new CSClientData(getSender(), this.id, msg.askUUID);
        this.updateLogger.logRequest(writeRequestUUID,
                                     new CSUpdateData(msg.index, msg.value, false, clientData)
        );
        
        super.debug(
                "Received write request during election from client " + getSender().path().name() +
                        " with uuid " + writeRequestUUID.toString().substring(0, 8) +
                        ", storing update locally until a coordinator is elected.");
        
    }
    
    /**
     * Special handler for write requests sent directly to the coordinator.
     * The write request is processed with the update protocol so that the update is applied to all
     * replicas.
     * This handler is used only by the coordinator.
     *
     * @param msg The write request sent by the client.
     */
    private void handleWriteRequestCoordinator(CSWriteRequest msg) {
        // To let this replica know which update key will be assigned by the coordinator, a UUID is
        // generated and appended to the request. The coordinator will send the UPDATE message
        // containing both the update key and the UUID.
        UUID writeRequestUUID = UUID.randomUUID();
        
        // The update is logged in the local update list of the coordinator
        var clientData = new CSClientData(getSender(), this.id, msg.askUUID);
        this.updateLogger.logRequest(writeRequestUUID,
                                     new CSUpdateData(msg.index, msg.value, false, clientData)
        );
        
        super.debug(
                "Received write request from client " + getSender().path().name() + " with uuid " +
                        writeRequestUUID.toString().substring(0, 8));
        
        this.updateNew(new CSWriteForward(msg, writeRequestUUID, clientData));
    }
    
    /**
     * Handler to receive requests forwarded to the coordinator.
     * This handler is used only by the coordinator.
     *
     * @param msg The write request forwarded by a replica to the coordinator.
     */
    private void handleWriteForward(CSWriteForward msg) {
        super.debug("Received forwarded request with uuid " +
                            msg.writeRequestUUID.toString().substring(0, 8));
        this.updateNew(msg);
    }
    
    // =================================================================================
    // Update Protocol
    // =================================================================================
    
    
    private void updateWithKey(
            CSUpdateKey updateKey, CSUpdateData data, UUID writeRequestUUID, UUID askUUID) {
        // The number of received ACKs for this update is initialized
        this.receivedAcks.put(updateKey, 1); // coordinator immediately acks to itself
        
        // This map contains all the replicas that don't already know the existence of this update
        var otherReplicas = new HashMap<>(this.replicas);
        otherReplicas.remove(this.coordinatorId); // same as this.id because update is executed only by the coordinator
        otherReplicas.remove(data.clientData.contactedReplicaId);
        
        ReplicaHandler handler = (replicaId, res, timedOut) -> {
            if (!timedOut) {
                super.debug("Replica " + replicaId + " ACKed update " + updateKey);
                this.receivedAcks.put(updateKey, this.receivedAcks.get(updateKey) + 1);
                
                // slash with integers always floor
                if (this.receivedAcks.get(updateKey) ==
                        this.replicas.size() / 2 + 1) { // happens only once because of ==
                    super.debug("Quorum reached for update " + updateKey + ", sending writeOks");
                    
                    this.sendWriteOk(updateKey);
                }
            } else {
                int crashedReplicaId = replicaId;
                
                super.debug("Replica " + crashedReplicaId + " didn't ACK update " + updateKey +
                                    " in time, must have crashed!");
                
                this.replicas.remove(crashedReplicaId);
                
                // Calculate the two replicas that need to update their pointers
                int previousReplicaId = this.getPreviousOf(crashedReplicaId);
                int nextReplicaId = this.getNextOf(crashedReplicaId);
                
                if (this.id == previousReplicaId) {
                    // If the coordinator is the replica before the one which crashed, it updates the next pointer
                    super.debug(
                            "I'm the replica before the one which crashed! Updating next pointer to " +
                                    nextReplicaId);
                    this.next = nextReplicaId;
                } else {
                    // Otherwise it sends a crash notice to the replica before the one which crashed
                    this.replicas.get(previousReplicaId)
                                 .tell(new CSCrashNotice(nextReplicaId), getSelf());
                }
            }
        };
        
        // The coordinator sends the UPDATE message to the replica which forwarded the request
        // note: coordinator never sends messages to itself
        // always handles updates by managing internal state
        if (this.coordinatorId != data.clientData.contactedReplicaId) {
            super.debug(
                    "Sending update " + updateKey + " to the replica which forwarded the request");
            ActorRef contactedReplica = this.replicas.get(data.clientData.contactedReplicaId);
            if (contactedReplica != null) {
                askSystem.<CSAck>ask(new CSUpdate(updateKey, data, writeRequestUUID, askUUID),
                                     contactedReplica,
                                     this.defaultTimeout,
                                     (res, timedOut) -> {
                                         handler.handle(data.clientData.contactedReplicaId,
                                                        res,
                                                        timedOut
                                         );
                                     }
                );
                
                if (this.crashSystem.shouldCrashAfterThisUpdate()) {
                    this.becomeCrashed();
                }
            }
        }
        
        // The coordinator sends the UPDATE message to all other replicas
        this.multicast(otherReplicas,
                       () -> new CSUpdate(updateKey, data, writeRequestUUID),
                       this.defaultTimeout,
                       handler
        );
        
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
    private void updateNew(CSWriteForward msg) {
        // Deep copy to avoid working on actor state
        CSUpdateKey updateKey = new CSUpdateKey(this.updateKey);
        
        super.debug("Sending update (P[" + msg.request.index + "] = " + msg.request.value +
                            " to replicas (key: " + updateKey + ", uuid: " +
                            msg.writeRequestUUID.toString().substring(0, 8) + ")");
        
        
        // The coordinator logs the update in its local update list
        CSUpdateData data = new CSUpdateData(msg.request.index,
                                             msg.request.value,
                                             false,
                                             msg.clientData
        );
        UUID writeRequestUUID = msg.writeRequestUUID;
        UUID askUUID = msg.askUUID;
        this.updateLogger.logUpdate(updateKey, writeRequestUUID, data);
        this.updateLogger.logRequest(writeRequestUUID, data);
        
        this.updateWithKey(updateKey, data, writeRequestUUID, askUUID);
        
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
        this.askSystem.handleResponse(msg); // Needed for the replica that forwarded the update to the coordinator
        
        if (this.updateLogger.isUpdateCompleted(msg.key)) {
            super.debug("I already applied this update, I'll just ACK the sender");
        } else {
            // The replica logs the update in its local update list and sends an ACK back to the coordinator
            this.updateLogger.logUpdate(msg.key, msg.writeRequestUUID, msg.data);
        }
        
        // The replica needs to save the askUUID related to this update
        // It is later used when handling the WriteOk message to run the lambda correctly
        this.updatesAskUUIDs.put(msg.key, msg.askUUID);
        this.askSystem.<CSWriteOk>ask(new CSAck(msg.askUUID),
                                      getSender(),
                                      this.defaultTimeout,
                                      (res, timedOut) -> {
                                          if (!timedOut) {
                                              if (!this.updateLogger.isUpdateCompleted(res.key)) {
                                                  this.completeUpdate(res.key);
                                                  this.sendWriteResult(res.key);
                                              } else {
                                                  super.debug(
                                                          "I already applied this update, no need to reapply it again");
                                              }
                                          } else {
                                              if (!this.updateLogger.isUpdateCompleted(msg.key)) {
                                                  this.startElection();
                                              }
                                          }
                                      }
        );
    }
    
    private void sendWriteOk(CSUpdateKey key) {
        super.debug("Sending WriteOks for update " + key);
        
        this.broadcast(new CSWriteOk(key));
        
        //super.debug("Status of update logger: " + this.logger);
        
        if (this.updateLogger.isUpdateCompleted(key)) {
            super.debug("I already applied this update, no need to reapply it again");
        } else {
            this.completeUpdate(key);
            this.sendWriteResult(key);
        }
    }
    
    /**
     * Handler to receive WRITEOK messages ({@link CSWriteOk}) sent by the coordinator.
     * This handler is used only by normal replicas.
     *
     * @param msg The message coming from the coordinator.
     */
    public final void handleWriteOk(CSWriteOk msg) {
        super.debug("Received WriteOk with key: " + msg.key);
        
        // The replica retrieves the correct askUUID related to the update
        UUID askUUID = this.updatesAskUUIDs.get(msg.key);
        // CSWroteOk is recreated containing the right askUUID
        this.askSystem.handleResponse(new CSWriteOk(msg.key, askUUID));
    }
    
    /**
     * This method writes an update to the {@code positions} array and uses the callback of the
     * codebase.
     * This operation is done by normal replicas and coordinator.
     *
     * @param key The update key of the update to be completed.
     */
    public void completeUpdate(CSUpdateKey key) {
        //super.debug("Writing to positions");
        CSUpdateData update = this.updateLogger.getUpdateData(key);
        this.positions[update.index] = update.value;
        callbackOnUpdateApplied(update.index, update.value);
        this.updateLogger.setCompleted(key);
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
        CSUpdateData update = this.updateLogger.getUpdateData(key);
        CSClientData clientData = update.clientData;
        
        if (this.id == clientData.contactedReplicaId) {
            super.debug("Sending WriteResult for " + key + " to client " +
                                clientData.actor.path().name() + " (askUUID: " +
                                clientData.askUUID + ")");
            super.tell(new CSWriteResult(update.index,
                                         update.value,
                                         true,
                                         this.id,
                                         clientData.askUUID
                       ), clientData.actor
            );
        } else {
            super.debug("I'm not the original handler of update " + key +
                                ", no need to send result to any client");
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

//        super.debug("Coordinator HeartBeat");
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
        this.heartBeatReceived = true;
        
        //super.debug("Received Coordinator HeartBeat, setting flag to true");
        
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
            super.debug(
                    "No heartbeat received from coordinator in the last period! Starting election.");
            this.startElection();
        }
    }
    
    // =================================================================================
    // CRASH
    // =================================================================================
    
    @Override
    public void crash(AbstractReplica.Crash how_to_crash) {
        super.debug("Received crash instructions: after " + how_to_crash.after_n_messages_of_type +
                            " messages of type " + how_to_crash.type.name());
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
            case CSUpdate m -> {
                return this.crashSystem.shouldCrashAfterThisUpdate();
            }
            case CSWriteOk m -> {
                return this.crashSystem.shouldCrashAfterThisWriteOk();
            }
            default -> {
                return false;
            }
        }
    }
    
    private int getPreviousOf(int replicaId) {
        return this.replicas.keySet()
                            .stream()
                            .filter(id -> id < replicaId)
                            .max(Integer::compare)
                            .orElse(Collections.min(this.replicas.keySet()));
    }
    
    private int getNextOf(int replicaId) {
        return this.replicas.keySet()
                            .stream()
                            .filter(id -> id > replicaId)
                            .min(Integer::compare)
                            .orElse(Collections.max(this.replicas.keySet()));
    }
    
    /**
     * Handler used by normal replicas to update the {@code next} reference
     * for the ring topology used during election.
     * This handler is used only by normal replicas.
     *
     * @param msg A message sent by the coordinator that specifies the new next
     *            replica.
     */
    public final void handleCrashNotice(CSCrashNotice msg) {
        super.debug("Received crash notice");
        
        this.replicas.remove(this.next);
        super.debug("Updating next pointer to " + msg.next);
        this.next = msg.next;
    }
    
    // =================================================================================
    // ELECTION
    // =================================================================================
    
    public void startElection() {
        if (!electing) {
            this.becomeElector();
            callbackOnElectionStarted(this.coordinatorId);
            
            this.replicas.remove(this.coordinatorId);
            if (this.next == this.coordinatorId) {
                this.next = this.getNextOf(this.next);
            }
            
            HashMap<Integer, CSUpdateKey> lastUpdates = new HashMap<>();
            lastUpdates.put(this.id, this.updateLogger.getLastUpdateKey());
            HashMap<Integer, CSUpdateKey> lastCompleteUpdates = new HashMap<>();
            lastCompleteUpdates.put(this.id, this.updateLogger.getLastCompleteUpdateKey());
            CSElection electionMsg = new CSElection(lastUpdates,
                                                    lastCompleteUpdates,
                                                    this.id,
                                                    this.coordinatorId
            );
            
            this.electionInitiatorId = this.id;
            
            this.sendIncompleteElectionMsg(electionMsg);
        }
    }
    
    public void sendIncompleteElectionMsg(CSElection msg) {
        super.debug("Forwarding incomplete election message started by " + msg.initiatorId +
                            " to replica " + this.next);
        
        askSystem.<CSAck>ask(new CSElection(msg),
                             replicas.get(this.next),
                             this.defaultTimeout,
                             (res, timedOut) -> {
                                 if (timedOut) {
                                     super.debug("Replica " + this.next +
                                                         " didn't ACK the election started from " +
                                                         msg.initiatorId +
                                                         ", forwarding to the next one in the ring.");
                                     // The next replica crashed, so the message must be sent to the following one in the ring
                                     this.replicas.remove(this.next);
                                     this.next = this.getNextOf(this.next);
                                     this.sendIncompleteElectionMsg(msg);
                                 } else {
                                     // If the ACK is received as expected, the replica just needs to wait for the election message to come back
                                 }
                             }
        );
    }
    
    // ACK the sender
    // If the initiatorId is smaller than the tracked one:
    //      Do nothing (the election initiated by the replica with the highest ID is the one to track)
    // Otherwise [1]:
    //      If the replica ID is not in the message yet:
    //          Add it and forward to next
    //      Otherwise [2]:
    //          If the winner of the election is itself:
    //              Send synchronization message and update replicas
    //          Otherwise [3]:
    //              Forward to next
    //              If the next is the winner but it doesn't ACK, remove it from the updateList and go back to 2
    public void handleElection(CSElection msg) {
        super.tell(new CSAck(msg.askUUID), getSender());
        
        // If the replica wasn't already in election state, enters it
        if (!electing) {
            this.becomeElector();
            callbackOnElectionStarted(msg.crashedCoordinatorId);
        }
        
        if (msg.initiatorId < this.electionInitiatorId) {
            super.debug("Ignoring election initiated by " + msg.initiatorId +
                                ", still tracking election from " + this.electionInitiatorId);
        } else {
            if (!msg.lastUpdates.containsKey(this.id)) {
                // The election message must complete the ring
                HashMap<Integer, CSUpdateKey> lastUpdates = new HashMap<>(msg.lastUpdates);
                lastUpdates.put(this.id, this.updateLogger.getLastUpdateKey());
                HashMap<Integer, CSUpdateKey> lastCompleteUpdates = new HashMap<>(msg.lastCompleteUpdates);
                lastCompleteUpdates.put(this.id, this.updateLogger.getLastCompleteUpdateKey());
                CSElection electionMsg = new CSElection(lastUpdates,
                                                        lastCompleteUpdates,
                                                        msg.initiatorId,
                                                        msg.crashedCoordinatorId
                );
                
                this.electionInitiatorId = msg.initiatorId;
                
                this.sendIncompleteElectionMsg(electionMsg);
            } else {
                this.evaluateCompleteElection(msg);
            }
        }
    }
    
    public int computeElectionWinner(Map<Integer, CSUpdateKey> lastUpdates) {
        return lastUpdates.entrySet().stream().max((a, b) -> {
            int cmp = a.getValue().compareTo(b.getValue());
            if (cmp != 0) {
                return cmp;
            } else {
                // spec: "replica identifiers are used to break ties when multiple replicas are equally up to date"
                return Integer.compare(a.getKey(), b.getKey());
            }
        }).orElseThrow().getKey();
    }
    
    public void evaluateCompleteElection(CSElection msg) {
        super.debug("Evaluating election started by " + msg.initiatorId + " with: " + msg);
        // The election message completed a ring
        if (this.id == computeElectionWinner(msg.lastUpdates)) {
            super.debug("I won the election initiated by " + msg.initiatorId + "!");
            
            callbackOnCoordinatorElected(this.id);
            
            this.becomeCoordinator();
            
            // Setting the starting update key with epoch increased
            int previousEpoch = msg.lastUpdates.entrySet()
                                               .stream()
                                               .max(Map.Entry.comparingByValue())
                                               .orElseThrow()
                                               .getValue().epoch;
            this.updateKey = new CSUpdateKey(previousEpoch + 1, 0);
            
            this.synchronizeAndUpdate(msg.lastCompleteUpdates);
        } else {
            this.sendCompleteElectionMsg(msg);
        }
    }
    
    public void sendCompleteElectionMsg(CSElection msg) {
        super.debug("Forwarding complete election message initiated by " + msg.initiatorId +
                            " to replica " + this.next);
        
        askSystem.<CSAck>ask(new CSElection(msg),
                             replicas.get(this.next),
                             this.defaultTimeout,
                             (res, timedOut) -> {
                                 if (timedOut) {
                                     int electionWinner = this.computeElectionWinner(msg.lastUpdates);
                                     
                                     if (this.next == electionWinner) {
                                         super.debug("Replica " + this.next +
                                                             " didn't ACK the election initiated by " +
                                                             msg.initiatorId +
                                                             ", but should become the coordinator! Removing it from the election candidates.");
                                         // The elected replica crashed before becoming coordinator
                                         this.replicas.remove(this.next);
                                         this.next = this.getNextOf(this.next);
                                         HashMap<Integer, CSUpdateKey> lastUpdates = new HashMap<>(
                                                 msg.lastUpdates);
                                         lastUpdates.remove(electionWinner);
                                         HashMap<Integer, CSUpdateKey> lastCompleteUpdates = new HashMap<>(
                                                 msg.lastCompleteUpdates);
                                         lastCompleteUpdates.remove(electionWinner);
                                         CSElection newElectionMsg = new CSElection(lastUpdates,
                                                                                    lastCompleteUpdates,
                                                                                    msg.initiatorId,
                                                                                    msg.crashedCoordinatorId
                                         );
                                         // Check if new winner is itself or should forward
                                         this.evaluateCompleteElection(newElectionMsg);
                                     } else {
                                         super.debug("Replica " + this.next +
                                                             " didn't ACK the complete initiated started by " +
                                                             msg.initiatorId +
                                                             ", forwarding to the next one in the ring.");
                                         // The next replica crashed, so the message must be sent to the following one in the ring
                                         this.replicas.remove(this.next);
                                         this.next = this.getNextOf(this.next);
                                         this.sendCompleteElectionMsg(msg);
                                     }
                                 } else {
                                     // If the ACK is received as expected, the replica just needs to wait for the synchronization message to arrive
                                 }
                             }
        );
    }
    
    public void synchronizeAndUpdate(Map<Integer, CSUpdateKey> lastCompleteUpdates) {
        super.debug("Sending synchronization message to replicas");
        
        this.broadcast(new CSSynchronization(this.id));
        
        List<CSUpdateKey> incompleteUpdateKey = this.updateLogger.getUpdateKeysAfter(Collections.min(
                lastCompleteUpdates.values()));
        
        // Propagating updates that already had an update key (assigned by the previous coordinator)
        for (var key : incompleteUpdateKey) {
            super.debug("Bringing replicas up-to-date for update " + key);
            
            // askUUID is not needed in this case, so it can be set to a dummy object
            this.updateWithKey(key,
                               this.updateLogger.getUpdateData(key),
                               this.updateLogger.getUUID(key),
                               UUID.randomUUID()
            );
        }
        
        // Propagating updates that this replica stored locally during election
        List<Map.Entry<UUID, CSUpdateData>> updates = this.updateLogger.getUpdatesWithoutKey();
        
        for (var update : updates) {
            // Assigning key to the update, storing it and propagating the update
            CSUpdateKey updateKey = new CSUpdateKey(this.updateKey);
            this.updateLogger.logUpdate(updateKey, update.getKey(), update.getValue());
            
            super.debug("Sending update that was requested during election (P[" +
                                update.getValue().index + "] = " + update.getValue().value +
                                " to replicas (key: " + updateKey + ", uuid: " +
                                update.getKey().toString().substring(0, 8) + ")");
            
            // askUUID is not needed in this case, so it can be set to a dummy object
            this.updateWithKey(updateKey, update.getValue(), update.getKey(), UUID.randomUUID());
            
            // The key to be assigned to the next update is saved
            this.updateKey = new CSUpdateKey(this.updateKey.epoch, this.updateKey.seq_no + 1);
        }
    }
    
    /**
     * Method to be fired to change the status of a replica to coordinator.
     */
    private void becomeCoordinator() {
        super.debug("Becoming coordinator");
        // Reset variables used for election
        this.electing = false;
        this.electionInitiatorId = -1;
        // Remove callbacks from previous state
        this.askSystem.cancelAllCallbacks();
        
        this.coordinatorId = this.id;
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
    
    private void becomeElector() {
        // Remove callbacks from previous state
        this.askSystem.cancelAllCallbacks();
        if (this.heartBeatTimer != null) {
            this.heartBeatTimer.cancel();
            this.heartBeatTimer = null;
        }
        getContext().become(elector());
        this.electing = true;
    }
    
    private void handleSynchronization(CSSynchronization msg) {
        super.debug("Received synchronization from " + msg.newCoordinatorId);
        
        this.coordinatorId = msg.newCoordinatorId;
        
        super.callbackOnCoordinatorElected(this.coordinatorId);
        
        // Become replica again
        this.becomeReplica();
        
        // Forward to the new coordinator updates that still have no updateKey (unprocessed update requests)
        List<Map.Entry<UUID, CSUpdateData>> updates = this.updateLogger.getUpdatesWithoutKey();
        
        for (var update : updates) {
            super.debug("Forwarding unprocessed update with UUID " +
                                update.getKey().toString().substring(0, 8) +
                                " to the new coordinator");
            
            askSystem.<CSUpdate>ask(new CSWriteForward(new CSWriteRequest(update.getValue().index,
                                                                          update.getValue().value
                                    ), update.getKey(), update.getValue().clientData
                                    ), replicas.get(this.coordinatorId), this.defaultTimeout, (res, timedOut) -> {
                                        if (timedOut) {
                                            // If the UPDATE message is not received, the coordinator must have crashed
                                            super.debug("No update received after forwarding request with uuid " +
                                                                update.getKey().toString().substring(0, 8) +
                                                                ". Starting election.");
                                            startElection();
                                        } else {
                                            //super.debug("Forward did not time out");
                                        }
                                    }
            );
        }
    }
    
    private void becomeReplica() {
        // Reset variables used for election
        this.electing = false;
        this.electionInitiatorId = -1;
        // Remove callbacks from previous state
        this.askSystem.cancelAllCallbacks();
        
        getContext().become(createReceive());
        
        // Setting up heartbeat system
        this.heartBeatReceived = false;
        this.heartBeatTimer = getContext().system()
                                          .scheduler()
                                          .scheduleWithFixedDelay(Duration.ofMillis(
                                                                          AbstractReplica.COORDINATOR_BEAT_INTERVAL / 2),
                                                                  Duration.ofMillis(
                                                                          AbstractReplica.COORDINATOR_BEAT_INTERVAL +
                                                                                  super.getMaxLatencyPlusTolerance()),
                                                                  getSelf(),
                                                                  new CSHeartBeatCheck(),
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
                                         .match(CSHeartBeatFromCoordinator.class,
                                                this::handleHeartBeatFromCoordinator
                                         )
                                         .match(CSHeartBeatCheck.class, this::handleHeartBeatCheck)
                                         .match(CSElection.class, this::handleElection)
                                         
                                         // hybrid
                                         .match(CSWriteOk.class, this::handleWriteOk)
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
                                         .match(CSElection.class, this::justAck)
                                         .match(CSSynchronization.class, this::messageBlackHole)
                                         
                                         // ask handlers
                                         .match(CSAck.class, askSystem::handleResponse)
                                         .match(CSAskTimeout.class, askSystem::handleTimeout)
                                         .build();
    }
    
    public Receive elector() {
        return createBaseReceiveBuilder().match(CSElection.class, this::handleElection)
                                         .match(CSSynchronization.class,
                                                this::handleSynchronization
                                         )
                                         .match(CSReadRequest.class, this::handleReadRequest)
                                         .match(CSWriteRequest.class,
                                                this::handleWriteRequestDuringElection
                                         )
                                         .match(CSWriteForward.class, this::messageBlackHole)
                                         .match(CSCrashNotice.class, this::messageBlackHole)
                                         .match(CSUpdate.class, this::messageBlackHole)
                                         .match(CSWriteOk.class, this::messageBlackHole)
                                         .match(CSHeartBeatFromCoordinator.class,
                                                this::messageBlackHole
                                         )
                                         .match(CSHeartBeatCheck.class, this::messageBlackHole)
                                         
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
                               .match(CSElection.class, this::messageBlackHole)
                               .match(CSSynchronization.class, this::messageBlackHole)
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
    
    private void justAck(CSAskMessage msg) {
        this.tell(new CSAck(msg.askUUID), getSender());
    }
}