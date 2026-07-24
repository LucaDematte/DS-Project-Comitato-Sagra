package it.unitn.ds.cs.messages.coordinator;

import it.unitn.ds.cs.CSUpdateKey;
import it.unitn.ds.cs.messages.CSAskMessage;

import java.util.UUID;

/**
 * Message sent by the coordinator during the update protocol to tell replicas to apply the update
 * with the specified key.
 * This method extends {@link CSAskMessage} so that it can be used with the custom ask system (more
 * info at {@link it.unitn.ds.cs.CSAsk}).
 */
public class CSWriteOk extends CSAskMessage {
    private final CSUpdateKey key;
    
    public CSWriteOk(CSUpdateKey key) {
        this.key = new CSUpdateKey(key);
    }
    
    public CSWriteOk(CSUpdateKey key, UUID askUUID) {
        super(askUUID);
        this.key = new CSUpdateKey(key);
    }
    
    public CSUpdateKey getKey() {
        return key;
    }
}
