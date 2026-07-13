package it.unitn.ds.cs;

import akka.actor.ActorRef;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class CSLogger {
    private final Map<CSUpdateKey, UUID> keyToUUIDBindings = new HashMap<>();
    private final Map<UUID, CSUpdateData> updates = new HashMap<>();
    
    /**
     * Record is automatically final
     *
     * @param actor
     * @param contactedReplicaId TODO: find out if there is a better place where to store this value
     * @param askUUID
     */
    public record CSClientData(ActorRef actor, int contactedReplicaId, UUID askUUID) {}
    
    private final Map<UUID, CSClientData> uuidToClientBindings = new HashMap<>();
    
    /**
     *
     * @param writeRequestUUID
     * @param update
     * @param client             The sender of the write request
     * @param contactedReplicaId
     * @param askUUID            This is used after receiving the WriteOk
     *                           to send the ack to che client with the correct askUUID
     */
    public void logRequest(
            UUID writeRequestUUID, CSUpdateData update, ActorRef client, int contactedReplicaId,
            UUID askUUID
    ) {
        if (!this.updates.containsKey(writeRequestUUID)) {
            this.updates.put(writeRequestUUID, update);
        }
        if (!this.uuidToClientBindings.containsKey(writeRequestUUID)) {
            this.uuidToClientBindings.put(writeRequestUUID,
                    new CSClientData(client, contactedReplicaId, askUUID));
        }
    }
    
    /**
     *
     * @param key
     * @param writeRequestUUID
     * @param update
     */
    public void logUpdate(CSUpdateKey key, UUID writeRequestUUID, CSUpdateData update) {
        if (!this.updates.containsKey(writeRequestUUID)) {
            this.updates.put(writeRequestUUID, update);
            this.keyToUUIDBindings.put(key, writeRequestUUID);
        } else {
            this.keyToUUIDBindings.put(key, writeRequestUUID);
        }
    }
    
    public CSUpdateData getUpdateData(CSUpdateKey key) {
        return this.updates.get(this.keyToUUIDBindings.get(key));
    }
    
    public CSClientData getClientData(CSUpdateKey key) {
        return this.uuidToClientBindings.get(this.keyToUUIDBindings.get(key));
    }
    
    public void setCompleted(CSUpdateKey key) {
        var old = this.updates.get(this.keyToUUIDBindings.get(key));
        this.updates.replace(this.keyToUUIDBindings.get(key),
                new CSUpdateData(old.index, old.value, true));
    }
}
