package it.unitn.ds.cs.messages.client;

import it.unitn.ds.cs.messages.CSAskMessage;

public class CSReadRequest extends CSAskMessage {
    public final int index;
    
    public CSReadRequest(int index) {
        super();
        this.index = index;
    }
}
