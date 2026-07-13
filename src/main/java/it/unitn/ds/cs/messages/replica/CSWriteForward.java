package it.unitn.ds.cs.messages.replica;

import it.unitn.ds.cs.messages.CSAskMessage;
import it.unitn.ds.cs.messages.client.CSWriteRequest;

import java.util.UUID;

public class CSWriteForward extends CSAskMessage {
    public final CSWriteRequest request;
    public final UUID writeRequestUUID;
    
    public CSWriteForward(CSWriteRequest request, UUID writeRequestUUID) {
        super();
        this.writeRequestUUID = writeRequestUUID;
        this.request = request;
    }
}
