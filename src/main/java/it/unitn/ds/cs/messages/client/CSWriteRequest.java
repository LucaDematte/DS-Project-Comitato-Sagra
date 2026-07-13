package it.unitn.ds.cs.messages.client;

import it.unitn.ds.cs.messages.CSAskMessage;

public class CSWriteRequest extends CSAskMessage {
    public final int index;
    public final int value;
    
    public CSWriteRequest(int index, int value) {
        super();
        this.index = index;
        this.value = value;
    }
}
