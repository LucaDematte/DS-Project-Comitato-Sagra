package it.unitn.ds.cs.messages.replica;

import it.unitn.ds.cs.messages.CSAskMessage;

import java.util.UUID;

/**
 * Message sent by a replica to a client, following a previous write request.
 * This method extends {@link CSAskMessage} so that it can be used with the custom ask system (more
 * info at {@link it.unitn.ds.cs.CSAsk}).
 */
public class CSWriteResult extends CSAskMessage {
    private final int index;
    private final int value;
    private final boolean success;
    private final int replicaId;
    
    public CSWriteResult(int index, int value, boolean success, int replicaId, UUID uuid) {
        super(uuid);
        this.index = index;
        this.value = value;
        this.success = success;
        this.replicaId = replicaId;
    }
    
    public int getIndex() {
        return index;
    }
    
    public int getValue() {
        return value;
    }
    
    public boolean isSuccess() {
        return success;
    }
    
    public int getReplicaId() {
        return replicaId;
    }
}
