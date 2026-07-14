package it.unitn.ds.cs.messages.replica;

import it.unitn.ds.cs.logger.CSClientData;
import it.unitn.ds.cs.messages.CSAskMessage;
import it.unitn.ds.cs.messages.client.CSWriteRequest;

import java.util.UUID;

public class CSWriteForward extends CSAskMessage {
    public final CSWriteRequest request;
    public final UUID writeRequestUUID;
    public final CSClientData clientData;
    
    public CSWriteForward(CSWriteRequest request, UUID writeRequestUUID, CSClientData clientData) {
        super();
        this.writeRequestUUID = writeRequestUUID;
        this.request = request;
        this.clientData = clientData;
    }
}
