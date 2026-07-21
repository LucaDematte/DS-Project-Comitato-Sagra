package it.unitn.ds.cs;

import it.unitn.ds.cs.logger.CSClientData;

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
