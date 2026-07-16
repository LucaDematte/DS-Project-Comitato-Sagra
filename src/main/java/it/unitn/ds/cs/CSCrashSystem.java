package it.unitn.ds.cs;

import it.unitn.ds.AbstractReplica;

import java.util.HashMap;

/**
 * The crash system maintains all crash instructions given to a replica and tracks the progress
 * towards crashes that must happen after a certain amount of processed messages of a certain kind.
 */
public class CSCrashSystem {
    /** This map keeps track of the number of messages left for each kind of crash registered. */
    private final HashMap<AbstractReplica.Crash.Type, Integer> crashInstructions;
    
    public CSCrashSystem() {
        this.crashInstructions = new HashMap<>();
    }
    
    /**
     * Registers a new crash type.
     * This method should be called whenever the replica receives a crash message
     * ({@link it.unitn.ds.AbstractReplica.Crash}).
     *
     * @param crash The object containing the crash instruction provided to the replica (the content
     *              of the crash message).
     */
    public void addInstruction(AbstractReplica.Crash crash) {
        this.crashInstructions.put(crash.type, crash.after_n_messages_of_type);
    }
    
    /**
     * Checks if a crash of type {@code Now} has been registered in the crash system.
     * This method should be called after addInstruction whenever the replica receives a crash
     * message.
     *
     * @return Whether the replica should crash after this method is called.
     */
    public boolean shouldCrashNow() {
        return this.crashInstructions.containsKey(AbstractReplica.Crash.Type.Now);
    }
    
    /**
     * Checks if a crash of type {@code Heartbeat} has been registered in the crash system.
     * This method should be called after the replica has received a HEARTBEAT message from the
     * coordinator.
     *
     * @return Whether the replica should crash after this method is called.
     */
    public boolean shouldCrashAfterThisHeartBeat() {
        return this.shouldCrashAfterThisEvent(AbstractReplica.Crash.Type.Heartbeat);
    }
    
    /**
     * Checks if a crash of type {@code Update} has been registered in the crash system.
     * This method should be called after the replica has processed an UPDATE message.
     *
     * @return Whether the replica should crash after this method is called.
     */
    public boolean shouldCrashAfterThisUpdate() {
        return this.shouldCrashAfterThisEvent(AbstractReplica.Crash.Type.Update);
    }
    
    /**
     * Checks if a crash of type {@code WriteOk} has been registered in the crash system.
     * This method should be called after the replica has processed a WRITEOK message.
     *
     * @return Whether the replica should crash after this method is called.
     */
    public boolean shouldCrashAfterThisWriteOk() {
        return this.shouldCrashAfterThisEvent(AbstractReplica.Crash.Type.WriteOK);
    }
    
    /**
     * Checks if a crash of type {@code Election} has been registered in the crash system.
     * This method should be called after the replica has processed a message related to the
     * election protocol.
     *
     * @return Whether the replica should crash after this method is called.
     */
    public boolean shouldCrashAfterThisElectionMessage() {
        return this.shouldCrashAfterThisEvent(AbstractReplica.Crash.Type.Election);
    }
    
    /**
     * Business logic to check if the replica should crash after a given event.
     * If the replica should crash, the method returns {@code true}, otherwise the number of
     * messages left before crashing stored in the map is decreased by one and {@code false} is
     * returned.
     *
     * @param event The kind of event that should be checked for a crash.
     * @return Whether the replica should crash after this method is called.
     */
    private boolean shouldCrashAfterThisEvent(AbstractReplica.Crash.Type event) {
        Integer messagesLeft = this.crashInstructions.get(event);
        if (messagesLeft == null) {
            // If no instruction is present for a certain kind of event, the replica should not crash.
            return false;
        } else {
            this.crashInstructions.put(event, messagesLeft - 1);
        }
        
        // The replica should crash if the number of messages left (for that kind of event) reaches zero.
        return messagesLeft - 1 <= 0;
    }
}
