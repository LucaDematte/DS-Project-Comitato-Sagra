package it.unitn.ds.cs.messages.replica;

import java.io.Serializable;

public class CSWriteResult implements Serializable {
    public final boolean success;
    public final int index;
    public final int value;
    public final int replicaId;
    
    public CSWriteResult(boolean success, int index, int value, int replicaId) {
        this.success = success;
        this.index = index;
        this.value = value;
        this.replicaId = replicaId;
    }
}
