package it.unitn.ds.cs.messages.coordinator;

import it.unitn.ds.cs.CSAsk;
import it.unitn.ds.cs.CSUpdateKey;

import java.util.UUID;

public class CSWriteOk extends CSAsk {
    public final CSUpdateKey key;
    
    public CSWriteOk(CSUpdateKey key, UUID uuid) {
        super(uuid);
        this.key = key;
    }
}
