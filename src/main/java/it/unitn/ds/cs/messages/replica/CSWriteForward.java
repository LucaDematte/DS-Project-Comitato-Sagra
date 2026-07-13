package it.unitn.ds.cs.messages.replica;

import it.unitn.ds.cs.CSAsk;
import it.unitn.ds.cs.messages.client.CSWriteRequest;

import java.io.Serializable;
import java.util.UUID;

public class CSWriteForward extends CSAsk implements Serializable {
    
    public final CSWriteRequest request;
    
    public CSWriteForward(UUID uuid, CSWriteRequest request) {
        super(uuid);
        this.request = request;
    }
}
