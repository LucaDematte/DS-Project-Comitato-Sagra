package it.unitn.ds.cs.messages.replica;

import it.unitn.ds.cs.messages.CSAskMessage;

import java.util.UUID;

/**
 * Message sent by a replica to a client, following a previous write request.
 * This method extends {@link CSAskMessage} so that it can be used with the custom ask system (more
 * info at {@link it.unitn.ds.cs.CSAsk}).
 */
public class CSWriteResult extends CSAskMessage {
    public final int index;
    public final int value;
    public final boolean success;
    public final int replicaId;
    
    public CSWriteResult(int index, int value, boolean success, int replicaId, UUID uuid) {
        super(uuid);
        this.index = index;
        this.value = value;
        this.success = success;
        this.replicaId = replicaId;
    }
}
