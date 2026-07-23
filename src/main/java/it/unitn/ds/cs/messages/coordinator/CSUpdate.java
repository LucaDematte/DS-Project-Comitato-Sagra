package it.unitn.ds.cs.messages.coordinator;

import it.unitn.ds.cs.CSUpdateData;
import it.unitn.ds.cs.CSUpdateKey;
import it.unitn.ds.cs.messages.CSAskMessage;

import java.util.UUID;

/**
 * Message sent by the coordinator to replicas during the update protocol.
 * This method extends {@link CSAskMessage} so that it can be used with the custom ask system (more
 * info at {@link it.unitn.ds.cs.CSAsk}).
 */
public class CSUpdate extends CSAskMessage {
    /** The key assigned by the coordinator to this update. */
    public final CSUpdateKey key;
    /** The container for all the data about this update. */
    public final CSUpdateData data;
    /**
     * The ID that was assigned by the replica which received the request from the client.
     * That replica will use this ID to assign the update key to the right update in its log.
     */
    public final UUID writeRequestUUID;
    
    public CSUpdate(CSUpdateKey key, CSUpdateData data, UUID writeRequestUUID, UUID uuid) {
        super(uuid);
        this.key = key;
        this.data = data;
        this.writeRequestUUID = writeRequestUUID;
    }
    
    public CSUpdate(CSUpdateKey key, CSUpdateData data, UUID writeRequestUUID) {
        super();
        this.key = key;
        this.data = data;
        this.writeRequestUUID = writeRequestUUID;
    }
}
