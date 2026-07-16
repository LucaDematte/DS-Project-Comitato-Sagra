package it.unitn.ds.cs.logger;

import it.unitn.ds.cs.CSUpdateData;
import it.unitn.ds.cs.CSUpdateKey;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * This class can be used by replicas to keep a history of the updates they completed or that still
 * need to be applied to the {@code positions} array.
 * <p>
 * Updates are identified in two ways: when a replica first receives the request from the client,
 * it stores it associated to a new {@link UUID}. This {@link UUID} is propagated to the coordinator so that, when
 * is sends the UPDATE message with the update key, the replica can also identify updates based on
 * the update key.
 * </p>
 */
public class CSLogger {
    /** Bindings from update keys to update UUIDs. */
    private final Map<CSUpdateKey, UUID> keyToUUIDBindings = new HashMap<>();
    /** Map that keeps the history of updates, identifying them with UUIDs. */
    private final Map<UUID, CSUpdateData> updates = new HashMap<>();
    /** Map that stores additional information about the client which requested the update and the replica it contacted. */
    private final Map<UUID, CSClientData> uuidToClientBindings = new HashMap<>();
    
    /**
     * This method adds a new update request to the history. It must be called by the replica that received
     * the request from the client. The update added by this method is identifiable only by {@link UUID}.
     *
     * @param writeRequestUUID The {@link UUID} used by the replica to associate an update to the update key chosen by the coordinator.
     * @param update The data about the update request.
     * @param clientData The data about the client that issued the request.
     */
    public void logRequest(UUID writeRequestUUID, CSUpdateData update, CSClientData clientData) {
        if (!this.updates.containsKey(writeRequestUUID)) {
            this.updates.put(writeRequestUUID, update);
        }
        if (!this.uuidToClientBindings.containsKey(writeRequestUUID)) {
            this.uuidToClientBindings.put(writeRequestUUID, clientData);
        }
    }
    
    /**
     * This method adds a new update request to the history. It must be called by the replicas that receive
     * the update information by the coordinator. The update added by this method is identifiable both by
     * {@link UUID} and by the update key (the binding is done automatically).
     * <p>
     * Note: this method doesn't store information about the client, since other replicas don't need to know
     * the client (they will not be in charge of sending a response to it).
     * </p>
     * @param key The update key received from the coordinator along with the update data.
     * @param writeRequestUUID The {@link UUID} of the request (received along with the update information by the coordinator).
     * @param update The update information received by the coordinator.
     */
    public void logUpdate(CSUpdateKey key, UUID writeRequestUUID, CSUpdateData update) {
        if (!this.updates.containsKey(writeRequestUUID)) {
            this.updates.put(writeRequestUUID, update);
            this.keyToUUIDBindings.put(key, writeRequestUUID);
        } else {
            this.keyToUUIDBindings.put(key, writeRequestUUID);
        }
    }
    
    /**
     * Retrieves an update from the history given an update key.
     * @param key The update key.
     * @return The update data associated with the given update key.
     */
    public CSUpdateData getUpdateData(CSUpdateKey key) {
        return this.updates.get(this.keyToUUIDBindings.get(key));
    }
    
    /**
     * Retrieves the client data of an update from the history given an update key.
     * @param key The update key.
     * @return The client data associated with the given update key.
     */
    public CSClientData getClientData(CSUpdateKey key) {
        return this.uuidToClientBindings.get(this.keyToUUIDBindings.get(key));
    }
    
    /**
     * Checks if some client data is associated to the give update key.
     * @param key The update key.
     * @return Whether the client data is present or not.
     */
    public boolean containsClientData(CSUpdateKey key) {
        return this.uuidToClientBindings.containsKey(this.keyToUUIDBindings.get(key));
    }
    
    /**
     * Marks an update of the history as completed.
     * This method should be called when the update information is applied to the {@code positions} array.
     * @param key The update key of the update to be set as complete.
     */
    public void setCompleted(CSUpdateKey key) {
        var old = this.updates.get(this.keyToUUIDBindings.get(key));
        this.updates.replace(this.keyToUUIDBindings.get(key),
                new CSUpdateData(old.index, old.value, true));
    }
}
