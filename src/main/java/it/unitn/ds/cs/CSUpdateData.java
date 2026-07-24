package it.unitn.ds.cs;

import java.io.Serializable;

/**
 * Container for all the data about an update that replicas log in their update history.
 */
public class CSUpdateData implements Serializable {
    private final int index;
    private final int value;
    private final boolean completed;
    private final CSClientData clientData;
    
    public CSUpdateData(int index, int value, boolean completed, CSClientData clientData) {
        this.index = index;
        this.value = value;
        this.completed = completed;
        this.clientData = clientData;
    }
    
    public CSUpdateData(CSUpdateData data) {
        this(data.index, data.value, data.completed, new CSClientData(data.clientData));
    }
    
    public int getIndex() {
        return index;
    }
    
    public int getValue() {
        return value;
    }
    
    public boolean isCompleted() {
        return completed;
    }
    
    public CSClientData getClientData() {
        return clientData;
    }
}
