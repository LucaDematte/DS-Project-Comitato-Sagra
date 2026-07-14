package it.unitn.ds.cs.logger;

import it.unitn.ds.cs.CSUpdateData;
import it.unitn.ds.cs.CSUpdateKey;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class CSLogger {
    private final Map<CSUpdateKey, UUID> keyToUUIDBindings = new HashMap<>();
    private final Map<UUID, CSUpdateData> updates = new HashMap<>();
    private final Map<UUID, CSClientData> uuidToClientBindings = new HashMap<>();
    
    public void logRequest(UUID writeRequestUUID, CSUpdateData update, CSClientData clientData) {
        if (!this.updates.containsKey(writeRequestUUID)) {
            this.updates.put(writeRequestUUID, update);
        }
        if (!this.uuidToClientBindings.containsKey(writeRequestUUID)) {
            this.uuidToClientBindings.put(writeRequestUUID, clientData);
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
    
    public boolean containsClientData(CSUpdateKey key) {
        return this.uuidToClientBindings.containsKey(this.keyToUUIDBindings.get(key));
    }
    
    public void setCompleted(CSUpdateKey key) {
        var old = this.updates.get(this.keyToUUIDBindings.get(key));
        this.updates.replace(this.keyToUUIDBindings.get(key),
                new CSUpdateData(old.index, old.value, true));
    }
}
