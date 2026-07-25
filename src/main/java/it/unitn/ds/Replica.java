package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds.cs.*;
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

// TODO cosa mettiamo nel main?
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
    int[] positions = new int[AbstractReplica.POSITIONS_LIST_LENGTH];
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
    /** A common timeout duration used across the project. */
    Duration defaultTimeout;
    /**
     * Map to associate a UUID for each update.
     * Used to implement the timeout check between the update message and the writeOk message for
     * the same update.
     */
    Map<CSUpdateKey, UUID> updatesAskUUIDs = new HashMap<>();
    
    /** A flag set to true when the replica is in the election state. */
    boolean electing = false;
    /**
     * This stores the ID of the replica that initiated the election that is currently being
     * tracked by this replica.
     * Since it may happen that multiple replicas start an election simultaneously, we have chosen
     * that a replica "cares" only about the election initiated by the replica with the highest ID.
     * When this replica receives an election message initiated by a replica with a lower ID than
     * the one stored in this field, the replica just ACKs the sender and does not forward it.
     * <p>
     * The field is initialized to -1 so that, at the beginning of an election, the replica starts
     * tracking an election initiated by any replica. When an election message initiated by a
     * replica with a higher ID is received, this field is updated. Finally, at the end of an
     * election, the field is reset to -1 in preparation for a future election.
     * </p>
     */
    int electionInitiatorId = -1; // this way the first election received is considered
    /** Timer for restarting the election if the process gets stuck. */
    Cancellable electionTimer = null;
    /**
     * Counter increased when a replica restarts the election after the election timeout expired.
     * By increasing the counter, other members won't ignore the election message.
     */
    int electionAttempt = 1;
    
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
            int result = this.positions[msg.getIndex()];
            tell(new CSReadResult(true, msg.getIndex(), result, this.id, msg.getAskUUID()),
                 getSender()
            );
        } catch (IndexOutOfBoundsException e) {
            tell(new CSReadResult(false, msg.getIndex(), 0, this.id, msg.getAskUUID()),
                 getSender()
            );
        }
    }
    
    // =================================================================================
    // WRITE REQUESTS
    // =================================================================================
    
    /**
     * Handler for write requests coming from clients.
     * A write request is resolved by forwarding it to the coordinator, which will use the update
     * protocol so that the update is applied to all replicas.
     * <p>
     * This replica will also start a timer so that, if no UPDATE message is received from the
     * coordinator, an election is started.
     * </p>
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
        var clientData = new CSClientData(getSender(), this.id, msg.getAskUUID());
        this.updateLogger.logRequest(writeRequestUUID,
                                     new CSUpdateData(msg.getIndex(),
                                                      msg.getValue(),
                                                      false,
                                                      clientData
                                     )
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
    
    /**
     * Handler for write requests received while an election is in progress.
     * Since there is no coordinator to which the request can be forwarded, the replica just logs
     * this update request locally.
     * When the election is completed, the replica will forward the updates to the new coordinator.
     * This way, even during elections, no update request is lost.
     *
     * @param msg
     */
    private void handleWriteRequestDuringElection(CSWriteRequest msg) {
        // To let this replica know which update key will be assigned by the coordinator, a UUID is
        // generated and appended to the request. The coordinator will send the UPDATE message
        // containing both the update key and the UUID.
        UUID writeRequestUUID = UUID.randomUUID();
        
        // The update is logged in the local update list of the replica
        var clientData = new CSClientData(getSender(), this.id, msg.getAskUUID());
        this.updateLogger.logRequest(writeRequestUUID,
                                     new CSUpdateData(msg.getIndex(),
                                                      msg.getValue(),
                                                      false,
                                                      clientData
                                     )
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
        var clientData = new CSClientData(getSender(), this.id, msg.getAskUUID());
        this.updateLogger.logRequest(writeRequestUUID,
                                     new CSUpdateData(msg.getIndex(),
                                                      msg.getValue(),
                                                      false,
                                                      clientData
                                     )
        );
        
        super.debug(
                "Received write request from client " + getSender().path().name() + " with uuid " +
                        writeRequestUUID.toString().substring(0, 8));
        
        // The update protocol is started for this request
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
                            msg.getWriteRequestUUID().toString().substring(0, 8));
        // The update protocol is started for this request
        this.updateNew(msg);
    }
    
    // =================================================================================
    // Update Protocol
    // =================================================================================
    
    /**
     * This method gathers and prepares all the information needed to perform the update protocol.
     * This method is used only by the coordinator.
     *
     * @param msg The write request forwarded by another replica (containing the information about
     *            the update).
     */
    private void updateNew(CSWriteForward msg) {
        // Deep copy to avoid working on actor state
        CSUpdateKey updateKey = new CSUpdateKey(this.updateKey);
        
        super.debug("Sending update (P[" + msg.getRequest().getIndex() + "] = " +
                            msg.getRequest().getValue() + " to replicas (key: " + updateKey +
                            ", uuid: " + msg.getWriteRequestUUID().toString().substring(0, 8) +
                            ")");
        
        CSUpdateData data = new CSUpdateData(msg.getRequest().getIndex(),
                                             msg.getRequest().getValue(),
                                             false,
                                             msg.getClientData()
        );
        UUID writeRequestUUID = msg.getWriteRequestUUID();
        UUID askUUID = msg.getAskUUID();
        // The coordinator logs the update in its local update list
        this.updateLogger.logUpdate(updateKey, writeRequestUUID, data);
        this.updateLogger.logRequest(writeRequestUUID, data);
        
        this.updateWithKey(updateKey, data, writeRequestUUID, askUUID);
        
        // The key to be assigned to the next update is saved
        this.updateKey = new CSUpdateKey(this.updateKey.epoch, this.updateKey.seqNo + 1);
    }
    
    /**
     * This method processes one update by following the specification of the update protocol.
     * A new UPDATE message ({@link CSUpdate}) with an update key ({@link CSUpdateKey}) is sent to
     * all replicas so that they get to know the update data.
     * Inside this method it is also defined the behavior of the coordinator when it receives an
     * ACK for the UPDATE message (inside a lambda).
     *
     * @param updateKey        The update key that has been assigned to this update.
     * @param data             The object containing all the data about this update.
     * @param writeRequestUUID The UUID which the replica that received the write request from the
     *                         client assigned to this update. This value is used by that replica to
     *                         associate the update key received with the UPDATE message to the
     *                         update request that is already present in its update log.
     * @param askUUID          The
     */
    private void updateWithKey(
            CSUpdateKey updateKey, CSUpdateData data, UUID writeRequestUUID, UUID askUUID) {
        // The number of received ACKs for this update is initialized
        this.receivedAcks.put(updateKey, 1); // coordinator immediately acks to itself
        
        // This map contains all the replicas that don't already know the existence of this update
        var otherReplicas = new HashMap<>(this.replicas);
        otherReplicas.remove(this.coordinatorId); // same as this.id because update is executed only by the coordinator
        otherReplicas.remove(data.getClientData().getContactedReplicaId());
        
        // This is the handler fired when ACKs for this update are received.
        ReplicaHandler handler = (replicaId, res, timedOut) -> {
            if (!timedOut) {
                super.debug("Replica " + replicaId + " ACKed update " + updateKey);
                this.receivedAcks.put(updateKey, this.receivedAcks.get(updateKey) + 1);
                
                // NOTE: - slash with integers always floor
                //       - happens only once because of ==
                if (this.receivedAcks.get(updateKey) == this.replicas.size() / 2 + 1) {
                    super.debug("Quorum reached for update " + updateKey + ", sending writeOks");
                    
                    // The quorum for this update has been reached, so the coordinator can send writeOks
                    this.sendWriteOk(updateKey);
                }
            } else {
                // When a replica didn't ACK the update in time, it is considered crashed, so the
                // coordinator sends a crash notice to the replica before it (so that it can update its next pointer)
                int crashedReplicaId = replicaId;
                
                super.debug("Replica " + crashedReplicaId + " didn't ACK update " + updateKey +
                                    " in time, must have crashed!");
                
                this.replicas.remove(crashedReplicaId);
                
                // Calculate the replicas adjacent to the one which crashed
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
        if (this.coordinatorId != data.getClientData().getContactedReplicaId()) {
            super.debug(
                    "Sending update " + updateKey + " to the replica which forwarded the request");
            ActorRef contactedReplica = this.replicas.get(data.getClientData()
                                                              .getContactedReplicaId());
            // Check that the replica is still in the group (not crashed)
            if (contactedReplica != null) {
                askSystem.<CSAck>ask(new CSUpdate(updateKey, data, writeRequestUUID, askUUID),
                                     contactedReplica,
                                     this.defaultTimeout,
                                     (res, timedOut) -> {
                                         handler.handle(data.getClientData()
                                                            .getContactedReplicaId(), res, timedOut
                                         );
                                     }
                );
                
                // The coordinator checks if it should crash after sending this update
                if (this.crashSystem.shouldCrashAfterThisUpdate()) {
                    this.becomeCrashed();
                }
            }
        }
        
        // The coordinator sends the UPDATE message to all other replicas
        // NOTE: checks for crash instructions are performed inside the multicast method
        this.multicast(otherReplicas,
                       () -> new CSUpdate(updateKey, data, writeRequestUUID),
                       this.defaultTimeout,
                       handler
        );
        
    }
    
    /**
     * Handler to receive UPDATE messages ({@link CSUpdate}) sent by the coordinator.
     * This handler is used only by normal replicas.
     *
     * @param msg The update message coming from the coordinator.
     */
    private void handleUpdate(CSUpdate msg) {
        super.debug("Received Update with key:" + msg.getKey());
        this.askSystem.handleResponse(msg); // Needed for the replica that forwarded the update to the coordinator
        
        if (this.updateLogger.isUpdateCompleted(msg.getKey())) {
            // A replica might have already applied this update if this update is being re-transmitted after the end of an election
            // The update is already applied if this replica is one of those that received the writeOk message before the coordinator crashed
            super.debug("I already applied this update, I'll just ACK the sender");
        } else {
            // The replica logs the update in its local update list
            this.updateLogger.logUpdate(msg.getKey(), msg.getWriteRequestUUID(), msg.getData());
        }
        
        // The replica needs to save the askUUID related to this update
        // It is later used when handling the WriteOk message to run the lambda correctly
        this.updatesAskUUIDs.put(msg.getKey(), msg.getAskUUID());
        
        // The replica sends the ACK back to the coordinator and starts a timer for the writeOk message
        // If the writeOk message for this update is not received in time, the coordinator is considered crashed and an election is started
        this.askSystem.<CSWriteOk>ask(new CSAck(msg.getAskUUID()),
                                      getSender(),
                                      this.defaultTimeout,
                                      (res, timedOut) -> {
                                          if (!timedOut) {
                                              // When the writeOk is received, if the replica didn't already apply the update (before a coordinator crash)
                                              // it applies it and checks if it should send the write result to the client
                                              if (!this.updateLogger.isUpdateCompleted(res.getKey())) {
                                                  this.completeUpdate(res.getKey());
                                                  this.sendWriteResult(res.getKey());
                                              } else {
                                                  super.debug(
                                                          "I already applied this update, no need to reapply it again");
                                              }
                                          } else {
                                              if (!this.updateLogger.isUpdateCompleted(msg.getKey())) {
                                                  this.startElection();
                                              }
                                          }
                                      }
        );
        
        // The replica checks if it should crash after processing this update
        if (this.crashSystem.shouldCrashAfterThisUpdate()) {
            this.becomeCrashed();
        }
    }
    
    /**
     * Method used by the coordinator to send WRITEOK messages to normal replicas.
     *
     * @param key The key of the update that gets added to the message.
     */
    private void sendWriteOk(CSUpdateKey key) {
        super.debug("Sending WriteOks for update " + key);
        
        this.broadcast(new CSWriteOk(key));
        
        //super.debug("Status of update logger: " + this.logger);
        
        // The coordinator applies the update locally (if he didn't already apply it under another coordinator
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
        super.debug("Received WriteOk with key: " + msg.getKey());
        
        // The replica retrieves the correct askUUID related to the update
        UUID askUUID = this.updatesAskUUIDs.get(msg.getKey());
        // CSWriteOk is recreated containing the right askUUID (so that the ask system can call the right callback)
        this.askSystem.handleResponse(new CSWriteOk(msg.getKey(), askUUID));
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
        this.positions[update.getIndex()] = update.getValue();
        callbackOnUpdateApplied(update.getIndex(), update.getValue());
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
        CSClientData clientData = update.getClientData();
        
        if (this.id == clientData.getContactedReplicaId()) {
            super.debug("Sending WriteResult for " + key + " to client " +
                                clientData.getActor().path().name() + " (askUUID: " +
                                clientData.getAskUUID() + ")");
            super.tell(new CSWriteResult(update.getIndex(),
                                         update.getValue(),
                                         true,
                                         this.id,
                                         clientData.getAskUUID()
                       ), clientData.getActor()
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
        // Cancel the timer for the election timeout
        if (this.electionTimer != null) {
            this.electionTimer.cancel();
            this.electionTimer = null;
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
    
    /**
     * Method used to get the active replica that is immediately before the one with the ID passed
     * as input.
     * This method is useful in the election process to know which replica is before another in the
     * ring topology defined by IDs.
     *
     * @param replicaId The replica ID of which the previous is required.
     * @return The ID of the replica before the one given as input.
     */
    private int getPreviousOf(int replicaId) {
        return this.replicas.keySet()
                            .stream()
                            .filter(id -> id < replicaId)
                            .max(Integer::compare)
                            .orElse(Collections.max(this.replicas.keySet()));
    }
    
    /**
     * Method used to get the active replica that is immediately after the one with the ID passed
     * as input.
     * This method is useful in the election process to know which replica is after another in the
     * ring topology defined by IDs.
     *
     * @param replicaId The replica ID of which the next is required.
     * @return The ID of the replica after the one given as input.
     */
    private int getNextOf(int replicaId) {
        return this.replicas.keySet()
                            .stream()
                            .filter(id -> id > replicaId)
                            .min(Integer::compare)
                            .orElse(Collections.min(this.replicas.keySet()));
    }
    
    /**
     * Handler used by normal replicas to update the {@code next} reference for the ring topology
     * used during election.
     * This handler is used only by normal replicas.
     *
     * @param msg A message sent by the coordinator that specifies the new next replica.
     */
    public final void handleCrashNotice(CSCrashNotice msg) {
        super.debug("Received crash notice");
        
        super.debug("Updating next pointer to " + msg.getNext());
        this.next = msg.getNext();
    }
    
    // =================================================================================
    // ELECTION
    // =================================================================================
    
    /**
     * This method starts the election process on the current replica.
     * The following actions are performed:
     * <ul>
     *     <li>The status of the replica is changed to the election state</li>
     *     <li>The callback of the codebase for the election start is fired</li>
     *     <li>The current coordinator (whose crash must be the cause for the election) is removed from the replicas map</li>
     *     <li>A new election message is built, containing the last update applied by this replica</li>
     *     <li>The method to send an incomplete election message is called</li>
     * </ul>
     */
    public void startElection() {
        if (!electing) {
            this.becomeElector();
            callbackOnElectionStarted(this.coordinatorId);
            
            // Removing the crashed coordinator (and eventually update the next pointer)
            this.replicas.remove(this.coordinatorId);
            if (this.next == this.coordinatorId) {
                this.next = this.getNextOf(this.next);
            }
            
            // Creating a new election message
            HashMap<Integer, CSUpdateKey> lastUpdates = new HashMap<>();
            lastUpdates.put(this.id, this.updateLogger.getMostRecentUpdateKey());
            HashMap<Integer, CSUpdateKey> lastCompleteUpdates = new HashMap<>();
            lastCompleteUpdates.put(this.id, this.updateLogger.getLastCompleteUpdateKey());
            CSElection electionMsg = new CSElection(lastUpdates,
                                                    lastCompleteUpdates,
                                                    this.id,
                                                    this.electionAttempt,
                                                    this.coordinatorId
            );
            
            this.electionInitiatorId = this.id;
            
            this.sendIncompleteElectionMsg(electionMsg);
            
            // The replica checks if it should crash after sending this election message
            if (this.crashSystem.shouldCrashAfterThisElectionMessage()) {
                this.becomeCrashed();
            }
        }
    }
    
    /**
     * This method contains the logic to correctly send an incomplete election message to the next
     * replica in the ring topology.
     * <p>
     * NOTE: an "incomplete" election message is an election message that still has to be
     * integrated with data of some replicas.
     * Alternatively, it is an election message that has yet to complete the first loop in the ring
     * topology.
     * </p>
     *
     * @param msg The message that has to be sent to the next replica in the ring.
     */
    public void sendIncompleteElectionMsg(CSElection msg) {
        super.debug("Forwarding incomplete election message started by " + msg.getInitiatorId() +
                            " to replica " + this.next);
        
        // The message is sent with a timer so that if the next replica doesn't ACK the message, the election message is not lost
        askSystem.<CSAck>ask(new CSElection(msg),
                             replicas.get(this.next),
                             this.defaultTimeout,
                             (res, timedOut) -> {
                                 if (timedOut) {
                                     super.debug("Replica " + this.next +
                                                         " didn't ACK the election started from " +
                                                         msg.getInitiatorId() +
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
    
    /**
     * Handler for election messages received by this replica.
     * The operations performed when calling this method are the following:
     * <ol>
     *     <li>ACK the sender</li>
     *     <li>
     *         If the {@code initiatorId} is smaller than the tracked one:
     *         <ul><li>Do nothing (the election initiated by the replica with the highest ID is the one to track)</li></ul>
     *     </li>
     *     <li>
     *         Otherwise:
     *         <ol>
     *             <li>
     *                 If this replica's ID is not in the message yet:
     *                 <ul><li>Add it with the last update information and forward to {@code next}</li></ul>
     *             </li>
     *             <li>
     *                 Otherwise:
     *                 <ol>
     *                     <li>
     *                         If the winner of the election is itself:
     *                         <ul><li>Send SYNCHRONIZATION message and update replicas</li></ul>
     *                     </li>
     *                     <li>
     *                         Otherwise:
     *                         <ul>
     *                             <li>Forward to {@code next}</li>
     *                             <li>If the next is the winner but it doesn't ACK, remove it from the {@code updateList} and go back to 3.2</li>
     *                         </ul>
     *                     </li>
     *                 </ol>
     *             </li>
     *         </ol>
     *     </li>
     * </ol>
     *
     * @param msg The election message received by this replica.
     */
    public void handleElection(CSElection msg) {
        super.tell(new CSAck(msg.getAskUUID()), getSender());
        
        // If the replica wasn't already in election state, enters it
        if (!electing) {
            this.becomeElector();
            callbackOnElectionStarted(msg.getCrashedCoordinatorId());
        }
        
        // Check if this election message has to be considered
        if (msg.getElectionAttempt() <= this.electionAttempt &&
                msg.getInitiatorId() < this.electionInitiatorId) {
            super.debug("Ignoring election initiated by " + msg.getInitiatorId() +
                                ", still tracking election from " + this.electionInitiatorId);
        } else {
            // Check if this replica already added its information to the message
            if (!msg.getLastUpdates().containsKey(this.id)) {
                // The election message must complete the ring
                HashMap<Integer, CSUpdateKey> lastUpdates = new HashMap<>(msg.getLastUpdates());
                lastUpdates.put(this.id, this.updateLogger.getMostRecentUpdateKey());
                HashMap<Integer, CSUpdateKey> lastCompleteUpdates = new HashMap<>(msg.getLastCompleteUpdates());
                lastCompleteUpdates.put(this.id, this.updateLogger.getLastCompleteUpdateKey());
                CSElection electionMsg = new CSElection(lastUpdates,
                                                        lastCompleteUpdates,
                                                        msg.getInitiatorId(),
                                                        msg.getElectionAttempt(),
                                                        msg.getCrashedCoordinatorId()
                );
                
                this.electionInitiatorId = msg.getInitiatorId();
                
                this.sendIncompleteElectionMsg(electionMsg);
                
                // The replica checks if it should crash after sending this election message
                if (this.crashSystem.shouldCrashAfterThisElectionMessage()) {
                    this.becomeCrashed();
                }
            } else {
                // If the message is complete, calculate the winner
                this.evaluateCompleteElection(msg);
            }
        }
    }
    
    /**
     * Given a map containing the most recent update key of each replica, this method returns the
     * ID
     * of the replica that won the election.
     * The replica that wins an election is the one that knows the most recent update, so that it
     * can bring the other replicas up-to-date.
     * Ties between replicas are resolved by taking the replica with the lower ID.
     *
     * @param lastUpdates The map extracted by an election message, containing the most recent
     *                    update key known by each replica.
     * @return The ID of the replica that won the election.
     */
    public int computeElectionWinner(Map<Integer, CSUpdateKey> lastUpdates) {
        return lastUpdates.entrySet().stream().max((a, b) -> {
            int cmp = a.getValue().compareTo(b.getValue());
            if (cmp != 0) {
                return cmp;
            } else {
                // spec: "replica identifiers are used to break ties when multiple replicas are equally up to date"
                return Integer.compare(b.getKey(), a.getKey());
            }
        }).orElseThrow().getKey();
    }
    
    /**
     * This method must be fired when a replica received a complete election message.
     * <p>
     * NOTE: a "complete" election message is an election message that contains the most recent
     * update information for all the active replicas in the system.
     * Alternatively, it is an election message that has already completed an entire loop in the
     * ring topology.
     * </p>
     * The method checks if this replica has won the election and, in that case, starts the
     * procedure to turn this replica into the new coordinator.
     * Otherwise, the election message is forwarded to the next replica in the ring so that the
     * complete election message reaches the winner.
     *
     * @param msg The complete election message to be analyzed.
     */
    public void evaluateCompleteElection(CSElection msg) {
        super.debug("Evaluating election started by " + msg.getInitiatorId() + " with: " + msg);
        // The election message completed a ring
        if (this.id == computeElectionWinner(msg.getLastUpdates())) {
            super.debug("I won the election initiated by " + msg.getInitiatorId() + "!");
            
            callbackOnCoordinatorElected(this.id);
            
            this.becomeCoordinator();
            
            // Setting the starting update key with epoch increased
            int previousEpoch = msg.getLastUpdates()
                                   .entrySet()
                                   .stream()
                                   .max(Map.Entry.comparingByValue())
                                   .orElseThrow()
                                   .getValue().epoch;
            this.updateKey = new CSUpdateKey(previousEpoch + 1, 0);
            
            // Sending the SYNCHRONIZATION message and bringing the replicas up-to-date
            this.synchronizeAndUpdate(msg.getLastCompleteUpdates());
        } else {
            this.sendCompleteElectionMsg(msg);
            
            // The replica checks if it should crash after sending this election message
            if (this.crashSystem.shouldCrashAfterThisElectionMessage()) {
                this.becomeCrashed();
            }
        }
    }
    
    /**
     * This method contains the logic to correctly send a complete election message to the next
     * replica in the ring topology.
     * <p>
     * NOTE: a "complete" election message is an election message that contains the most recent
     * update information for all the active replicas in the system.
     * Alternatively, it is an election message that has already completed an entire loop in the
     * ring topology.
     * </p>
     * If the next replica ACKs in time, this replica just needs to wait for a SYNCHRONIZATION
     * message from the replica that will become the coordinator.
     * Otherwise, the next replica must have crashed after adding its data to the election message.
     * In this case, its data is removed from the message.
     * If the next replica was also supposed to become the new coordinator, this replica
     * re-evaluates the election message after removing the data about the crashed replica.
     * This way, the election message will not loop forever, and it's not needed to redo the
     * election from scratch.
     *
     * @param msg The election message to be sent to the next replica.
     */
    public void sendCompleteElectionMsg(CSElection msg) {
        super.debug("Forwarding complete election message initiated by " + msg.getInitiatorId() +
                            " to replica " + this.next);
        
        askSystem.<CSAck>ask(new CSElection(msg),
                             replicas.get(this.next),
                             this.defaultTimeout,
                             (res, timedOut) -> {
                                 if (timedOut) {
                                     int electionWinner = this.computeElectionWinner(msg.getLastUpdates());
                                     
                                     // Check if the winner is the next replica (that didn't respond in time and caused this timeout)
                                     if (this.next == electionWinner) {
                                         super.debug("Replica " + this.next +
                                                             " didn't ACK the election initiated by " +
                                                             msg.getInitiatorId() +
                                                             ", but should become the coordinator! Removing it from the election candidates.");
                                         // The elected replica crashed before becoming coordinator
                                         this.replicas.remove(this.next);
                                         this.next = this.getNextOf(this.next);
                                         HashMap<Integer, CSUpdateKey> lastUpdates = new HashMap<>(
                                                 msg.getLastUpdates());
                                         lastUpdates.remove(electionWinner);
                                         HashMap<Integer, CSUpdateKey> lastCompleteUpdates = new HashMap<>(
                                                 msg.getLastCompleteUpdates());
                                         lastCompleteUpdates.remove(electionWinner);
                                         CSElection newElectionMsg = new CSElection(lastUpdates,
                                                                                    lastCompleteUpdates,
                                                                                    msg.getInitiatorId(),
                                                                                    msg.getElectionAttempt(),
                                                                                    msg.getCrashedCoordinatorId()
                                         );
                                         // Re-evaluate the message without the crashed replica's data
                                         this.evaluateCompleteElection(newElectionMsg);
                                     } else {
                                         super.debug("Replica " + this.next +
                                                             " didn't ACK the complete initiated started by " +
                                                             msg.getInitiatorId() +
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
    
    /**
     * This method is fired by the elected coordinator so that the election process is concluded and
     * other replicas can be brought up-to-date.
     *
     * @param lastCompleteUpdates The map of the most recent update known by each replica.
     */
    public void synchronizeAndUpdate(Map<Integer, CSUpdateKey> lastCompleteUpdates) {
        super.debug("Sending synchronization message to replicas");
        
        this.broadcast(new CSSynchronization(this.id));
        
        // The new coordinator takes all the updates that are after the least recent update known by the replicas
        // These updates will be reprocessed with the update protocol
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
        
        // Propagating new updates that this replica stored locally during election
        List<Map.Entry<UUID, CSUpdateData>> updates = this.updateLogger.getUpdatesWithoutKey();
        
        for (var update : updates) {
            // Assigning key to the update, storing it and propagating the update
            CSUpdateKey updateKey = new CSUpdateKey(this.updateKey);
            this.updateLogger.logUpdate(updateKey, update.getKey(), update.getValue());
            
            super.debug("Sending update that was requested during election (P[" +
                                update.getValue().getIndex() + "] = " +
                                update.getValue().getValue() + " to replicas (key: " + updateKey +
                                ", uuid: " + update.getKey().toString().substring(0, 8) + ")");
            
            // askUUID is not needed in this case, so it can be set to a dummy object
            this.updateWithKey(updateKey, update.getValue(), update.getKey(), UUID.randomUUID());
            
            // The key to be assigned to the next update is saved
            this.updateKey = new CSUpdateKey(this.updateKey.epoch, this.updateKey.seqNo + 1);
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
        this.electionAttempt = 1;
        // Remove callbacks from previous state
        this.askSystem.cancelAllCallbacks();
        // Cancel the timer for the election timeout
        if (this.electionTimer != null) {
            this.electionTimer.cancel();
            this.electionTimer = null;
        }
        
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
    
    /**
     * Method to be fired to change the status of a replica to the election state.
     */
    private void becomeElector() {
        // Remove callbacks from previous state
        this.askSystem.cancelAllCallbacks();
        if (this.heartBeatTimer != null) {
            this.heartBeatTimer.cancel();
            this.heartBeatTimer = null;
        }
        getContext().become(elector());
        this.electing = true;
        
        // Setting a timer to address cases where the election process gets stuck
        if (this.electionTimer != null) {
            this.electionTimer.cancel();
        }
        Duration timeout = Duration.ofMillis(
                this.defaultTimeout.toMillis() * this.replicas.size() * 2);
        this.electionTimer = getContext().system()
                                         .scheduler()
                                         .scheduleOnce(timeout,
                                                       getSelf(),
                                                       new CSElectionTimeout(),
                                                       getContext().system().dispatcher(),
                                                       getSelf()
                                         );
        
    }
    
    /**
     * Handler for election timeout messages.
     * When an election timeout message is received, it means that the replica is still in election
     * state by a long time, more than what is required to complete an election even in the worst
     * case.
     * This means that the election process must have got stuck, and so it must be restarted.
     *
     * @param msg The election timeout message this replica scheduled to itself at the beginning of
     *            the election.
     */
    private void handleElectionTimeout(CSElectionTimeout msg) {
        super.debug("Timeout for election completion has expired. Starting another election.");
        
        this.electing = false;
        this.electionAttempt += 1;
        this.startElection();
    }
    
    /**
     * Handler for the SYNCHRONIZATION message sent by the new coordinator.
     * When a replica receives this message, it exits the election state by becoming a normal
     * replica.
     * The new coordinator ID is saved and the replica starts to forward to the coordinator all the
     * new updates that were sent by clients during the election.
     *
     * @param msg The synchronization message received by the replica.
     */
    private void handleSynchronization(CSSynchronization msg) {
        super.debug("Received synchronization from " + msg.getNewCoordinatorId());
        
        this.coordinatorId = msg.getNewCoordinatorId();
        
        super.callbackOnCoordinatorElected(this.coordinatorId);
        
        // Become replica again
        this.becomeReplica();
        
        // Cancel the timer for the election timeout
        if (this.electionTimer != null) {
            this.electionTimer.cancel();
            this.electionTimer = null;
        }
        
        // Forward to the new coordinator updates that still have no updateKey (unprocessed update requests)
        List<Map.Entry<UUID, CSUpdateData>> updates = this.updateLogger.getUpdatesWithoutKey();
        
        for (var update : updates) {
            super.debug("Forwarding unprocessed update with UUID " +
                                update.getKey().toString().substring(0, 8) +
                                " to the new coordinator");
            
            askSystem.<CSUpdate>ask(new CSWriteForward(new CSWriteRequest(update.getValue()
                                                                                .getIndex(),
                                                                          update.getValue()
                                                                                .getValue()
                                    ), update.getKey(), update.getValue().getClientData()
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
    
    /**
     * Method to be fired to change the status of a replica to a normal replica.
     */
    private void becomeReplica() {
        // Reset variables used for election
        this.electing = false;
        this.electionInitiatorId = -1;
        this.electionAttempt = 1;
        // Remove callbacks from previous state
        this.askSystem.cancelAllCallbacks();
        
        getContext().become(createReceive());
        
        // Setting up heartbeat system
        this.heartBeatReceived = false;
        this.heartBeatTimer = getContext().system()
                                          .scheduler()
                                          .scheduleWithFixedDelay(Duration.ofMillis(
                                                                          AbstractReplica.COORDINATOR_BEAT_INTERVAL / 2),
                                                                  Duration.ofMillis(AbstractReplica.COORDINATOR_BEAT_INTERVAL),
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
                                         .match(CSElectionTimeout.class, this::messageBlackHole)
                                         
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
                                         .match(CSElectionTimeout.class, this::messageBlackHole)
                                         
                                         // ask handlers
                                         .match(CSAck.class, askSystem::handleResponse)
                                         .match(CSAskTimeout.class, askSystem::handleTimeout)
                                         .build();
    }
    
    /**
     * Creates and builds a Receive with the bindings for a replica in election state.
     *
     * @return The Receive object.
     */
    public Receive elector() {
        return createBaseReceiveBuilder().match(CSElection.class, this::handleElection)
                                         .match(CSSynchronization.class,
                                                this::handleSynchronization
                                         )
                                         .match(CSElectionTimeout.class,
                                                this::handleElectionTimeout
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
     * All messages are bound to a dummy handler that does nothing.
     * This way, Akka doesn't print warnings for "dead letters".
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
                               .match(CSElectionTimeout.class, this::messageBlackHole)
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
    
    /**
     * Handler used by the coordinator to ack election messages.
     * This is necessary because, when a replica becomes the coordinator, there may still be
     * election messages roaming around the ring topology.
     * These messages cannot just be ignored because the replicas still in election state may
     * interpret this as a replica crash, causing inconsistencies.
     *
     * @param msg The message that needs to be ACKed and ignored.
     */
    private void justAck(CSAskMessage msg) {
        this.tell(new CSAck(msg.getAskUUID()), getSender());
    }
}