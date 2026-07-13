package it.unitn.ds.cs.messages.replica;

import it.unitn.ds.cs.messages.CSAskMessage;

import java.util.UUID;

/**
 * Message sent by a replica to a client, following a previous read request.
 */
public class CSReadResult extends CSAskMessage {
    public final boolean success;
    public final int index;
    public final int value;
    public final int replicaId;
    
    public CSReadResult(boolean success, int index, int value, int replicaId, UUID requestUUID) {
        super(requestUUID);
        this.success = success;
        this.index = index;
        this.value = value;
        this.replicaId = replicaId;
    }
}
