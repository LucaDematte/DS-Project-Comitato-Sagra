package it.unitn.ds.cs.logger;

import it.unitn.ds.cs.CSUpdateData;
import it.unitn.ds.cs.CSUpdateKey;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This class can be used by replicas to keep a history of the updates they completed or that still
 * need to be applied to the {@code positions} array.
 * <p>
 * Updates are identified in two ways: when a replica first receives the request from the client,
 * it stores it associated to a new {@link UUID}.
 * This {@link UUID} is propagated to the coordinator so that, when it sends the UPDATE message with
 * the update key, the replica can also identify updates based on the update key.
 * </p>
 */
public class CSUpdateLogger {
    /** Bindings from update keys to update UUIDs. */
    private final Map<CSUpdateKey, UUID> keyToUUIDBindings = new HashMap<>();
    /** Map that keeps the history of updates, identifying them with UUIDs. */
    private final Map<UUID, CSUpdateData> updates = new HashMap<>();
    
    /**
     * This method adds a new update request to the history.
     * It must be called by the replica that received the request from the client.
     * The update added by this method is identifiable only by {@link UUID}.
     *
     * @param writeRequestUUID The {@link UUID} used by the replica to associate an update to the
     *                         update key chosen by the coordinator.
     * @param update           The data about the update request.
     */
    public void logRequest(UUID writeRequestUUID, CSUpdateData update) {
        if (!this.updates.containsKey(writeRequestUUID)) {
            this.updates.put(writeRequestUUID, update);
        }
    }
    
    /**
     * This method adds a new update request to the history.
     * It must be called by the replicas that receive the update information by the coordinator.
     * The update added by this method is identifiable both by {@link UUID} and by the update key
     * (the binding is done automatically).
     * <p>
     * Note: this method doesn't store information about the client, since other replicas don't
     * need to know the client (they will not be in charge of sending a response to it).
     * </p>
     *
     * @param key              The update key received from the coordinator along with the update
     *                         data.
     * @param writeRequestUUID The {@link UUID} of the request (received along with the update
     *                         information by the coordinator).
     * @param update           The update information received by the coordinator.
     */
    public void logUpdate(CSUpdateKey key, UUID writeRequestUUID, CSUpdateData update) {
        if (!this.updates.containsKey(writeRequestUUID)) {
            this.updates.put(writeRequestUUID, update);
            this.keyToUUIDBindings.put(key, writeRequestUUID);
        } else {
            this.keyToUUIDBindings.put(key, writeRequestUUID);
        }
    }
    
    public CSUpdateKey getUpdateKey(UUID uuid) {
        return this.keyToUUIDBindings.entrySet()
                                     .stream()
                                     .filter(entry -> entry.getValue().equals(uuid))
                                     .findFirst()
                                     .orElseThrow()
                                     .getKey();
    }
    
    /**
     * Retrieves an update from the history given an update key.
     *
     * @param key The update key.
     * @return The update data associated with the given update key.
     */
    public CSUpdateData getUpdateData(CSUpdateKey key) {
        return this.updates.get(this.keyToUUIDBindings.get(key));
    }
    
    public UUID getUUID(CSUpdateKey key) {
        return this.keyToUUIDBindings.get(key);
    }
    
    /**
     * Marks an update of the history as completed.
     * This method should be called when the update information is applied to the {@code positions}
     * array.
     *
     * @param key The update key of the update to be set as complete.
     */
    public void setCompleted(CSUpdateKey key) {
        var old = this.updates.get(this.keyToUUIDBindings.get(key));
        this.updates.replace(this.keyToUUIDBindings.get(key),
                             new CSUpdateData(old.index, old.value, true, old.clientData)
        );
    }
    
    /**
     * Check if an update associated to a key has been completed.
     *
     * @param key The update key.
     * @return If the update is completed or not.
     */
    public boolean isUpdateCompleted(CSUpdateKey key) {
        CSUpdateData data = this.updates.get(this.keyToUUIDBindings.get(key));
        return (data != null && data.completed);
    }
    
    public CSUpdateKey getLastUpdateKey() {
        if (this.keyToUUIDBindings.isEmpty()) {
            // If the replica doesn't have any update, return an update key that is "before" the first update
            return new CSUpdateKey(-1, -1);
        } else {
            return Collections.max(this.keyToUUIDBindings.keySet());
        }
    }
    
    public CSUpdateKey getLastCompleteUpdateKey() {
        Set<CSUpdateKey> keys = this.keyToUUIDBindings.keySet()
                                                      .stream()
                                                      .filter(key -> this.updates.get(this.keyToUUIDBindings.get(
                                                              key)).completed)
                                                      .collect(Collectors.toSet());
        if (keys.isEmpty()) {
            // If the replica doesn't have any update, return an update key that is "before" the first update
            return new CSUpdateKey(-1, -1);
        } else {
            return Collections.max(keys);
        }
    }
    
    public List<CSUpdateKey> getUpdateKeysAfter(CSUpdateKey key) {
        List<CSUpdateKey> keyList = new ArrayList<>(this.keyToUUIDBindings.keySet()
                                                                          .stream()
                                                                          .filter(k -> k.compareTo(
                                                                                  key) > 0)
                                                                          .toList());
        Collections.sort(keyList);
        return keyList;
    }
    
    public List<Map.Entry<UUID, CSUpdateData>> getUpdatesWithoutKey() {
        return this.updates.entrySet()
                           .stream()
                           .filter(entry -> !this.keyToUUIDBindings.containsValue(entry.getKey()))
                           .toList();
    }
    
    @Override
    public String toString() {
        StringBuilder rtn = new StringBuilder();
        for (var update : updates.entrySet()) {
            if (keyToUUIDBindings.containsValue(update.getKey())) {
                var binding = keyToUUIDBindings.entrySet()
                                               .stream()
                                               .filter((b) -> b.getValue() == update.getKey())
                                               .findFirst()
                                               .orElseThrow();
                rtn.append(binding.getKey());
            } else {
                rtn.append("????");
            }
            
            rtn.append(": P[")
               .append(update.getValue().index)
               .append("] = ")
               .append(update.getValue().value)
               .append(", ");
        }
        
        return rtn.toString();
    }
}
