package it.unitn.ds.cs.messages.coordinator;

import it.unitn.ds.cs.CSAsk;
import it.unitn.ds.cs.CSUpdateKey;
import it.unitn.ds.cs.CSUpdateValue;

import java.io.Serializable;
import java.util.UUID;

public class CSUpdate extends CSAsk implements Serializable {
    public final CSUpdateKey key;
    public final CSUpdateValue update;
    
    public CSUpdate(UUID uuid, CSUpdateKey key, CSUpdateValue update) {
        super(uuid);
        this.key = key;
        this.update = update;
    }
}
