package it.unitn.ds.cs;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class UpdateLog {
    private final Map<CSUpdateKey, UUID> keyToUUIDBindings;
    private final Map<UUID, CSUpdateData> updates;
    
    public UpdateLog() {
        this.keyToUUIDBindings = new HashMap<>();
        this.updates = new HashMap<>();
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
    
    public void logRequest(UUID writeRequestUUID, CSUpdateData update) {
        if (!this.updates.containsKey(writeRequestUUID)) {
            this.updates.put(writeRequestUUID, update);
        }
    }
    
    public CSUpdateData get(CSUpdateKey key) {
        return this.updates.get(this.keyToUUIDBindings.get(key));
    }
    
    public void setCompleted(CSUpdateKey key) {
        var old = this.updates.get(this.keyToUUIDBindings.get(key));
        this.updates.replace(this.keyToUUIDBindings.get(key),
                new CSUpdateData(old.index, old.value, true));
    }
}
