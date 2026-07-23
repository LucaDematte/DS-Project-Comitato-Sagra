package it.unitn.ds.cs;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This class can be used by replicas to keep a history of the updates they completed or that still
 * need to be applied to the {@code positions} array.
 * <p>
 * Updates are identified in two ways: when a replica first receives the request from the client,
 * it stores it associated to a new {@link UUID}.
 * This {@link UUID} is propagated to the coordinator so that, when it sends the UPDATE message
 * with the update key ({@link CSUpdateKey}), the replica can also identify updates based on the
 * update key.
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
    
    /**
     * Returns the update key of an update given its UUID.
     *
     * @param uuid The UUID of the update of which the update key is required.
     * @return The update key.
     */
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
    
    /**
     * Returns the UUID of an update given its update key.
     *
     * @param key The update key of the update of which the UUID is required.
     * @return The UUID of the update.
     */
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
     * Checks if an update associated to a key has been completed.
     *
     * @param key The update key.
     * @return If the update is completed or not.
     */
    public boolean isUpdateCompleted(CSUpdateKey key) {
        CSUpdateData data = this.updates.get(this.keyToUUIDBindings.get(key));
        return (data != null && data.completed);
    }
    
    /**
     * Returns the most recent update key that has been logged.
     * <p>
     * NOTE: updates that have not yet been assigned an update key are ignored, even if they have
     * been logged more recently.
     * </p>
     * If no update key is present in the log, [-1, -1] is returned.
     *
     * @return The most recent update key.
     */
    public CSUpdateKey getMostRecentUpdateKey() {
        if (this.keyToUUIDBindings.isEmpty()) {
            // If the replica doesn't have any update, return an update key that is "before" the first update
            return new CSUpdateKey(-1, -1);
        } else {
            return Collections.max(this.keyToUUIDBindings.keySet());
        }
    }
    
    /**
     * Returns the update key of the most recent update that has been completed (applied to the
     * {@code positions} array)
     * <p>
     * NOTE: updates that have not yet been assigned an update key are ignored, even if they have
     * been logged more recently.
     * </p>
     * If no update key is present in the log, [-1, -1] is returned.
     *
     * @return The update key ot the most recent completed update.
     */
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
    
    /**
     * Returns a sorted list of all update keys that are more recent than the one provided as input.
     *
     * @param key The lower bound for update keys to be returned.
     * @return The sorted list of update keys more recent than the one provided.
     */
    public List<CSUpdateKey> getUpdateKeysAfter(CSUpdateKey key) {
        List<CSUpdateKey> keyList = new ArrayList<>(this.keyToUUIDBindings.keySet()
                                                                          .stream()
                                                                          .filter(k -> k.compareTo(
                                                                                  key) > 0)
                                                                          .toList());
        Collections.sort(keyList);
        return keyList;
    }
    
    /**
     * Returns all updates that have not yet been assigned an update key.
     *
     * @return A list of map entries, each containing the update UUID as key and the update data as value.
     */
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
