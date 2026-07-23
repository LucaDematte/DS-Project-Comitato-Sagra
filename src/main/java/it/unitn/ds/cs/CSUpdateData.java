package it.unitn.ds.cs;

/**
 * Container for all the data about an update that replicas log in their update history.
 */
public class CSUpdateData {
    public final int index;
    public final int value;
    public final boolean completed;
    public final CSClientData clientData;
    
    public CSUpdateData(int index, int value, boolean completed, CSClientData clientData) {
        this.index = index;
        this.value = value;
        this.completed = completed;
        this.clientData = clientData;
    }
}
