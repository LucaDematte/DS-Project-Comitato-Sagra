package it.unitn.ds.cs.messages.client;

import it.unitn.ds.cs.messages.CSAskMessage;

/**
 * Message sent by clients when they want to write a {@code value} at a specific {@code index} in
 * the {@code positions} array.
 * This method extends {@link CSAskMessage} so that it can be used with the custom ask system (more
 * info at {@link it.unitn.ds.cs.CSAsk}).
 */
public class CSWriteRequest extends CSAskMessage {
    private final int index;
    private final int value;
    
    public CSWriteRequest(int index, int value) {
        super();
        this.index = index;
        this.value = value;
    }
    
    public CSWriteRequest(CSWriteRequest request) {
        super(request.getAskUUID());
        this.index = request.getIndex();
        this.value = request.getValue();
    }
    
    public int getIndex() {
        return index;
    }
    
    public int getValue() {
        return value;
    }
}
