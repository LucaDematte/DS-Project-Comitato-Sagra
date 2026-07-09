package it.unitn.ds.cs.messages;

import java.io.Serializable;

/**
 * Message sent by a replica to a client, following a previous read request.
 */
public class CSReadResult implements Serializable {
    public final boolean success;
    public final int index;
    public final int value;
    public final int replicaId;

    public CSReadResult(boolean success, int index, int value, int replicaId) {
        this.success = success;
        this.index = index;
        this.value = value;
        this.replicaId = replicaId;
    }
}
