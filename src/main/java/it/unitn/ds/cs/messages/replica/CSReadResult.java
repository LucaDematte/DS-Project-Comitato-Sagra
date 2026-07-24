package it.unitn.ds.cs.messages.replica;

import it.unitn.ds.cs.messages.CSAskMessage;

import java.util.UUID;

/**
 * Message sent by a replica to a client, following a previous read request.
 * This method extends {@link CSAskMessage} so that it can be used with the custom ask system (more
 * info at {@link it.unitn.ds.cs.CSAsk}).
 */
public class CSReadResult extends CSAskMessage {
    private final boolean success;
    private final int index;
    private final int value;
    private final int replicaId;
    
    public CSReadResult(boolean success, int index, int value, int replicaId, UUID requestUUID) {
        super(requestUUID);
        this.success = success;
        this.index = index;
        this.value = value;
        this.replicaId = replicaId;
    }
    
    public boolean isSuccess() {
        return success;
    }
    
    public int getIndex() {
        return index;
    }
    
    public int getValue() {
        return value;
    }
    
    public int getReplicaId() {
        return replicaId;
    }
}
