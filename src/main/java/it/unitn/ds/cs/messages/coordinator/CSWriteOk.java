package it.unitn.ds.cs.messages.coordinator;

import it.unitn.ds.cs.CSUpdateKey;
import it.unitn.ds.cs.messages.CSAskMessage;

import java.util.UUID;

public class CSWriteOk extends CSAskMessage {
    public final CSUpdateKey key;
    
    public CSWriteOk(CSUpdateKey key) {
        this.key = key;
    }
    
    public CSWriteOk(CSUpdateKey key, UUID askUUID) {
        super(askUUID);
        this.key = key;
    }
}
