package it.unitn.ds.cs.messages.client;

import it.unitn.ds.cs.messages.CSAskMessage;

/**
 * Message sent by clients when they want to perform a read at a specific index in the
 * {@code positions} array.
 * This method extends {@link CSAskMessage} so that it can be used with the custom ask system (more
 * info at {@link it.unitn.ds.cs.CSAsk}).
 */
public class CSReadRequest extends CSAskMessage {
    private final int index;
    
    public CSReadRequest(int index) {
        super();
        this.index = index;
    }
    
    public int getIndex() {
        return index;
    }
}
