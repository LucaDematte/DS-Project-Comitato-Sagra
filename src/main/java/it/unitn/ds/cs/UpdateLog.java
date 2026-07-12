package it.unitn.ds.cs;

import akka.actor.ActorRef;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class UpdateLog {
    private final Map<CSUpdateKey, UUID> uuidBindings;
    private final Map<UUID, UpdateData> updates;
    
    public UpdateLog() {
        this.uuidBindings = new HashMap<>();
        this.updates = new HashMap<>();
    }
    
    public UUID addLocal(UUID uuid, int index, int value, UpdateStatus status, ActorRef client) {
        return add(uuid, new UpdateData(index, value, status, true, client));
    }
    
    private UUID add(UUID uuid, UpdateData update) {
        this.updates.put(uuid, update);
        return uuid;
    }
    
    public UUID addRemote(CSUpdateKey key, UUID uuid, int index, int value) {
        if (this.updates.containsKey(uuid)) {
            //Logger.log("Remote update is already present in log, so it is local. Binding key to uuid");
            this.bindUpdateToKey(uuid, key);
            setProcessing(key);
            return this.uuidBindings.get(key);
        } else {
            //Logger.log("Adding new remote update with uuid " + uuid);
            add(uuid, new UpdateData(index, value, UpdateStatus.PROCESSING, false,
                    ActorRef.noSender()));
            bindUpdateToKey(uuid, key);
            return uuid;
        }
    }
    
    public void bindUpdateToKey(UUID uuid, CSUpdateKey key) {
        this.uuidBindings.put(key, uuid);
    }
    
    public UpdateData get(UUID uuid) {
        return this.updates.get(uuid);
    }
    
    public UpdateData get(CSUpdateKey key) {
        return this.updates.get(this.uuidBindings.get(key));
    }
    
    public UUID getUUIDBinding(CSUpdateKey key) {
        return this.uuidBindings.get(key);
    }
    
    public void setCompleted(CSUpdateKey key) {
        setCompleted(this.uuidBindings.get(key));
    }
    
    public void setCompleted(UUID uuid) {
        this.updates.get(uuid).status = UpdateStatus.COMPLETED;
    }
    
    public void setProcessing(CSUpdateKey key) {
        setProcessing(this.uuidBindings.get(key));
    }
    
    public void setProcessing(UUID uuid) {
        this.updates.get(uuid).status = UpdateStatus.PROCESSING;
    }
    
    public Set<UUID> getNonCompleteUpdateKeys() {
        return this.updates.entrySet()
                           .stream()
                           .filter(entry -> entry.getValue().status != UpdateStatus.COMPLETED)
                           .map(Map.Entry::getKey)
                           .collect(Collectors.toSet());
    }
    
}
