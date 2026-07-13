package it.unitn.ds.cs.messages.client;

import it.unitn.ds.cs.CSAsk;

import java.io.Serializable;
import java.util.UUID;

public class CSReadRequest extends CSAsk implements Serializable {
    public final int index;
    
    public CSReadRequest(UUID uuid, int index) {
        super(uuid);
        this.index = index;
    }
}
